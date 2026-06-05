import os
import json
import uuid
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)
from confluent_kafka import Producer

# ─── Schéma ──────────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  True),
    StructField("user_id",      StringType(),  True),
    StructField("track_id",     StringType(),  True),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  True),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])

# ─── State en mémoire ────────────────────────────────────────────────────────

user_state = {}
_kafka_producer = None

# ─── Producer Kafka ───────────────────────────────────────────────────────────

def get_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = Producer({
            "bootstrap.servers": "localhost:9092",
            "client.id": "spark-fraud-producer",
        })
    return _kafka_producer

# ─── Écriture PostgreSQL via docker exec ──────────────────────────────────────

POSTGRES_CONTAINER = "data_pipeline-postgres-1"

def pg_exec(sql):
    """Exécute une requête SQL dans PostgreSQL via docker exec."""
    result = subprocess.run(
        ["docker", "exec", "-i", POSTGRES_CONTAINER,
         "psql", "-U", "spotify", "-d", "spotify", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    return result.stdout


def write_to_postgres(alerts, batch_id):
    """Écrit les alertes dans fraud_detections."""
    try:
        for alert in alerts:
            evidence = json.dumps({
                "alert":           alert["alert"],
                "suspicion_score": alert["suspicion_score"],
            }).replace("'", "''")
            sql = f"""
                INSERT INTO fraud_detections
                    (id, user_id, fraud_type, suspicion_score, evidence, detected_at)
                VALUES (
                    gen_random_uuid(),
                    '{alert["user_id"]}',
                    '{alert["alert"]}',
                    {alert["suspicion_score"]},
                    '{evidence}'::jsonb,
                    NOW()
                );
            """
            pg_exec(sql)
        print(f"[Batch {batch_id}] ✓ {len(alerts)} alertes écrites dans PostgreSQL fraud_detections")
    except Exception as e:
        print(f"[Batch {batch_id}] ✗ Erreur PostgreSQL fraud_detections : {e}")


def write_to_dead_letter(invalid_rows, batch_id):
    """Écrit les événements invalides dans dead_letter_events."""
    try:
        for row in invalid_rows:
            payload = json.dumps(row).replace("'", "''")
            sql = f"""
                INSERT INTO dead_letter_events
                    (id, original_topic, payload, error_type, error_message, status, created_at)
                VALUES (
                    gen_random_uuid(),
                    'listening_events',
                    '{payload}'::jsonb,
                    'missing_user_id',
                    'user_id est null ou manquant',
                    'pending',
                    NOW()
                );
            """
            pg_exec(sql)
        print(f"[Batch {batch_id}] ✓ {len(invalid_rows)} événements écrits dans dead_letter_events")
    except Exception as e:
        print(f"[Batch {batch_id}] ✗ Erreur PostgreSQL dead_letter_events : {e}")


# ─── Logique de détection ────────────────────────────────────────────────────

def detect_fraud(batch_df, batch_id):
    # ── Dead Letter Queue : événements sans user_id ───────────────────────────
    invalid_pdf = batch_df.filter(col("user_id").isNull()).toPandas()
    if len(invalid_pdf) > 0:
        write_to_dead_letter(invalid_pdf.to_dict("records"), batch_id)

    # ── Traitement des événements valides ─────────────────────────────────────
    valid_df = batch_df.filter(col("user_id").isNotNull())
    count = valid_df.count()
    print(f"[Batch {batch_id}] {count} événements reçus")
    if count == 0:
        return

    pdf = valid_df.toPandas()
    alerts = []

    for user_id, group in pdf.groupby("user_id"):
        state = user_state.get(user_id, {
            "count":            0,
            "total_duration":   0,
            "short_count":      0,
            "incomplete_count": 0,
        })

        state["count"]            += len(group)
        state["total_duration"]   += int(group["duration_ms"].fillna(0).sum())
        state["short_count"]      += int((group["duration_ms"] < 5000).sum())
        state["incomplete_count"] += int((group["completed"] == False).sum())
        user_state[user_id]        = state

        n              = state["count"]
        total_duration = state["total_duration"]
        incomplete     = state["incomplete_count"]
        suspicion      = 0.0

        # Règle 1 : > 100 écoutes
        if n > 100:
            suspicion += 0.4
            alerts.append({
                "user_id":         user_id,
                "alert":           "High frequency: >100 listens",
                "suspicion_score": round(suspicion, 2),
            })

        # Règle 2 : durée moyenne < 5s
        if n > 10 and (total_duration / n) < 5000:
            suspicion += 0.4
            alerts.append({
                "user_id":         user_id,
                "alert":           "Abnormally short duration: avg <5s",
                "suspicion_score": round(suspicion, 2),
            })

        # Règle 3 : > 50% d'écoutes incomplètes
        if n > 5 and (incomplete / n) > 0.5:
            suspicion += 0.3
            alerts.append({
                "user_id":         user_id,
                "alert":           "High incomplete rate: >50%",
                "suspicion_score": round(suspicion, 2),
            })

    if not alerts:
        return

    print(f"[Batch {batch_id}] {len(alerts)} alerte(s) détectée(s)")

    # ── Publication Kafka ─────────────────────────────────────────────────────
    producer = get_producer()
    for alert in alerts:
        producer.produce(
            topic="fraud_alerts",
            key=alert["user_id"],
            value=json.dumps(alert),
        )
    producer.flush()
    print(f"[Batch {batch_id}] ✓ Alertes publiées dans Kafka fraud_alerts")

    # ── Écriture PostgreSQL ───────────────────────────────────────────────────
    write_to_postgres(alerts, batch_id)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    os.environ["HADOOP_HOME"]    = "C:/hadoop"
    os.environ["PATH"]          += ";C:/hadoop/bin"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.makedirs("C:/tmp/spark-checkpoints", exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("FraudDetection")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.checkpointLocation",
                "C:/tmp/spark-checkpoints")
        .config("spark.driver.host",        "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "listening_events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw_df
        .select(
            from_json(col("value").cast("string"), LISTENING_EVENT_SCHEMA)
            .alias("data")
        )
        .select("data.*")
    )

    query = (
        events.writeStream
        .outputMode("append")
        .foreachBatch(detect_fraud)
        .option("checkpointLocation", "C:/tmp/spark-checkpoints")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()