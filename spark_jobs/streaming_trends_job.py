"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                   org.postgresql:postgresql:42.7.1 \
        spark_jobs/streaming_trends_job.py
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.

    Retourne un DataFrame streaming avec colonnes typées et parsées.
    """
    # Lire depuis Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")  # ou "earliest" pour déboguer
        .load()
    )

    # Caster value (bytes) en string et parser le JSON
    parsed_df = (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                LISTENING_EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
    )

    # Caster timestamp (string ISO 8601) en TimestampType et renommer
    events_df = (
        parsed_df
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .drop("timestamp")
    )

    return events_df


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes.
    
    Fenêtre tumbling = pas de chevauchement, on reset toutes les 5 min.
    Écriture dans PostgreSQL via foreachBatch + JDBC.
    """
    
    # Agrégation par fenêtre de 5 min + track_id
    windowed_df = (
        events_df
        .filter(F.col("completed") == True)  # Seulement les écoutes complètes
        .groupBy(
            F.window(F.col("event_time"), "5 minutes"),
            F.col("track_id")
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.countDistinct("user_id").alias("unique_listeners")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("track_id"),
            F.col("stream_count"),
            F.col("unique_listeners")
        )
    )

    # Fonction pour écrire un batch dans PostgreSQL
    def write_to_postgres(batch_df, batch_id):
        """
        Écrit un batch de résultats dans PostgreSQL.
        """
        if batch_df.count() == 0:
            print(f"Batch {batch_id} : vide, rien à écrire")
            return

        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "realtime_top_tracks") \
            .options(**POSTGRES_PROPS) \
            .save()

        print(f"Batch {batch_id} : {batch_df.count()} lignes écrites dans realtime_top_tracks")

    # Écriture streaming avec foreachBatch
    query = (
        windowed_df
        .writeStream
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks")
        .outputMode("update")
        .start()
    )

    return query


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).
    
    Sliding window = fenêtres qui se chevauchent.
    Jointure stream-static avec le catalogue pour récupérer les genres.
    Écriture dans Redis via foreachBatch.
    """
    
    # Jointure stream-static : events avec catalogue pour ajouter le genre
    enriched_df = (
        events_df
        .join(
            catalog_df.select("id", "genre"),
            events_df.track_id == catalog_df.id,
            "left"
        )
        .filter(F.col("completed") == True)
    )

    # Sliding window : 15 min de durée, glisse toutes les 5 min
    windowed_df = (
        enriched_df
        .groupBy(
            F.window(F.col("event_time"), "15 minutes", "5 minutes"),
            F.col("genre")
        )
        .agg(
            F.countDistinct("user_id").alias("unique_listeners")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("genre"),
            F.col("unique_listeners")
        )
        .filter(F.col("genre").isNotNull())
    )

    # Fonction pour écrire un batch dans Redis
    def write_to_redis(batch_df, batch_id):
        """
        Écrit les résultats dans Redis.
        Clé : "genre_listeners:live" avec format JSON.
        """
        try:
            import redis
        except ImportError:
            print("redis-py non installé, installation recommandée : pip install redis")
            return

        if batch_df.count() == 0:
            print(f"Batch {batch_id} : vide, rien à écrire en Redis")
            return

        # Convertir en dict pour Redis
        records = batch_df.collect()
        genre_stats = {}

        for row in records:
            genre = row["genre"] or "unknown"
            listeners = row["unique_listeners"]
            
            # Garder seulement le dernier (on overwrite)
            genre_stats[genre] = {
                "unique_listeners": listeners,
                "window_start": str(row["window_start"]),
                "window_end": str(row["window_end"])
            }

        # Écrire dans Redis
        try:
            r = redis.Redis(host="redis", port=6379, db=1, decode_responses=True)
            r.setex(
                "genre_listeners:live",
                3600,  # TTL 1h
                json.dumps(genre_stats)
            )
            print(f"Batch {batch_id} : {len(genre_stats)} genres écrits dans Redis")
        except Exception as e:
            print(f"Erreur écriture Redis : {e}")

    # Écriture streaming avec foreachBatch
    query = (
        windowed_df
        .writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/genre_listeners")
        .outputMode("update")
        .start()
    )

    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka
    events_df = read_kafka_stream(spark)

    # Chargement du catalogue (jointure statique)
    try:
        catalog_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "tracks") \
            .options(**POSTGRES_PROPS) \
            .load()
        print("Catalogue chargé depuis PostgreSQL")
    except Exception as e:
        print(f"Erreur chargement catalogue : {e}")
        catalog_df = None

    # Agrégations
    query_top_tracks = compute_top_tracks_tumbling(events_df)
    
    if catalog_df is not None:
        query_genres = compute_genre_listeners_sliding(events_df, catalog_df)

    # Attendre l'arrêt gracieux
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()