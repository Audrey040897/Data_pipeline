import os
import json
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
KAFKA_LATE_TOPIC = "late_listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),
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
    Configure la session avec les paramètres S3A requis pour MinIO (Issue #13)
    et les paramètres réseau pour la Spark UI.
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # CONFIGURATION RÉSEAU (UI)
        #.config("spark.ui.port", "4040")
        #.config("spark.driver.bindAddress", "0.0.0.0")
        #.config("spark.driver.host", "localhost")
        # CONFIGURATION MINIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

# ─────────────────────────────────────────────────────────────
# LECTURE ET TRAITEMENT (Issue #13 & #15)
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lecture Kafka avec Watermark de 10 minutes (Issue #15).
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed") 
        .load()
    )

    events_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # Ajout du Watermark de 10 min (Livrable Issue #15)
    return events_df.withColumn(
        "event_time", 
        F.to_timestamp(F.col("timestamp"))
    ).withWatermark("event_time", "10 minutes")

def route_late_events(batch_df, batch_id):
    """
    Routage des événements tardifs vers Kafka (Livrable Issue #15).
    """
    max_time_row = batch_df.select(F.max("event_time")).collect()
    
    if max_time_row and max_time_row[0][0]:
        max_time = max_time_row[0][0]
        threshold = max_time - timedelta(minutes=10)
        
        late_df = batch_df.filter(F.col("event_time") < threshold)
        
        if late_df.count() > 0:
            try:
                (late_df
                 .select(F.to_json(F.struct("*")).alias("value"))
                 .write
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                 .option("topic", KAFKA_LATE_TOPIC)
                 .save())
                
                print(f"✅ BATCH {batch_id} : {late_df.count()} late events routés vers {KAFKA_LATE_TOPIC}")
            except Exception as e:
                print(f"❌ BATCH {batch_id} : Erreur lors du routage Kafka : {e}")
        else:
            print(f"ℹ️ BATCH {batch_id} : Pas d'événements en retard")

def compute_top_tracks_tumbling(events_df):
    """
    Validation Issue #15 : Affichage console ET routage via foreachBatch.
    """
    # 1. Affichage console pour validation visuelle (Critère Issue #15)
    query_console = (
        events_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_PATH + "_console")
        .start()
    )

    # 2. Routage vers Kafka topic 'late_listening_events' (Livrable Issue #15)
    query_routing = (
        events_df.writeStream
        .foreachBatch(route_late_events)
        .option("checkpointLocation", CHECKPOINT_PATH + "_routing")
        .start()
    )

    return query_console

# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("🚀 Démarrage du job Spark (Watermarking & Routing - Issue #15)")
    print("=" * 60)

    try:
        events_df = read_kafka_stream(spark)
        query = compute_top_tracks_tumbling(events_df)
        print("✅ Pipeline de streaming initialisé avec succès")
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"❌ Erreur critique lors de l'exécution du job : {e}")

if __name__ == "__main__":
    main()
