"""
DAG : streaming_events_pipeline
"""
import json
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import psycopg2
import redis

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline
Consomme en micro-batch les événements Redis, valide, enrichit et stocke.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

REDIS_URL        = os.getenv("REDIS_URL", "redis://redis:6379/1")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
POSTGRES_CONN_ID = "spotify_postgres"
BATCH_SIZE       = 1000  # max events par batch


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """Lit les events depuis les Redis LISTs (lpush côté simulateur)."""
        r = redis.from_url(REDIS_URL, decode_responses=True)

        listening = []
        p2p       = []

        # On lit jusqu'à BATCH_SIZE messages par channel
        for _ in range(BATCH_SIZE):
            msg = r.rpop("queue:listening_events")
            if msg is None:
                break
            try:
                listening.append(json.loads(msg))
            except json.JSONDecodeError:
                pass

        for _ in range(BATCH_SIZE):
            msg = r.rpop("queue:p2p_network_events")
            if msg is None:
                break
            try:
                p2p.append(json.loads(msg))
            except json.JSONDecodeError:
                pass

        print(f"Consommé : {len(listening)} listening events, {len(p2p)} p2p events")
        return {"listening": listening, "p2p_network": p2p}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """Valide les champs obligatoires, envoie les invalides en DLQ."""
        REQUIRED_FIELDS = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        valid_listening = []
        valid_p2p       = []
        errors          = []

        for event in raw_events.get("listening", []):
            missing = [f for f in REQUIRED_FIELDS if f not in event]
            if missing or not isinstance(event.get("duration_ms"), (int, float)) or event.get("duration_ms", 0) <= 0:
                errors.append({**event, "error_type": "validation", "error_detail": f"Champs manquants: {missing}"})
            else:
                valid_listening.append(event)

        for event in raw_events.get("p2p_network", []):
            if "event_id" not in event or "event_type" not in event:
                errors.append({**event, "error_type": "validation"})
            else:
                valid_p2p.append(event)

        # Envoyer les invalides en DLQ PostgreSQL
        if errors:
            try:
                hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                conn = hook.get_conn()
                cur  = conn.cursor()
                for e in errors:
                    cur.execute("""
                        INSERT INTO dead_letter_events (event_id, payload, error_type, created_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """, (e.get("event_id", "unknown"), json.dumps(e), e.get("error_type", "validation")))
                conn.commit()
                cur.close()
            except Exception as ex:
                print(f"DLQ indisponible : {ex}")

        print(f"Valides : {len(valid_listening)} listening, {len(valid_p2p)} p2p | Erreurs : {len(errors)}")
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p, "errors": len(errors)}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """Enrichit les events avec artiste et genre depuis PostgreSQL."""
        events = validated.get("valid_listening", [])
        if not events:
            return []

        track_ids = list({e["track_id"] for e in events})

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT t.id, t.title, a.name as artist_name, t.genre
                FROM tracks t
                LEFT JOIN artists a ON t.artist_id = a.id
                WHERE t.id = ANY(%s)
            """, (track_ids,))
            rows = cur.fetchall()
            cur.close()
        except Exception as ex:
            print(f"PostgreSQL indisponible : {ex}")
            return events

        catalog = {row[0]: {"track_title": row[1], "artist_name": row[2], "genre": row[3]} for row in rows}

        enriched = []
        for event in events:
            info = catalog.get(event["track_id"])
            if info:
                enriched.append({**event, **info})
            else:
                enriched.append({**event, "track_title": None, "artist_name": None, "genre": None})

        print(f"Enrichis : {len(enriched)} events")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """Sauvegarde en Parquet sur MinIO partitionné par date/heure."""
        if not enriched_events:
            print("Aucun event à stocker")
            return ""

        df       = pd.DataFrame(enriched_events)
        now      = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        run_id   = context["run_id"].replace(":", "-").replace("+", "-")
        path     = f"/tmp/part-{run_id}.parquet"
        key      = f"listening_events/date={date_str}/hour={hour_str}/part-{run_id}.parquet"

        df.to_parquet(path, index=False)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        # Créer le bucket si besoin
        try:
            s3.head_bucket(Bucket="spotify-parquet")
        except Exception:
            s3.create_bucket(Bucket="spotify-parquet")

        s3.upload_file(path, "spotify-parquet", key)
        print(f"Parquet uploadé : s3://spotify-parquet/{key}")
        return key

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """Insère les events dans PostgreSQL de façon idempotente."""
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        inserted = 0
        skipped  = 0

        for event in enriched_events:
            try:
                cur.execute("""
                    INSERT INTO listening_events
                        (id, user_id, track_id, timestamp,
                        duration_ms, device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event.get("event_id"),
                    event.get("user_id"),
                    event.get("track_id"),
                    event.get("timestamp"),
                    event.get("duration_ms"),
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed"),
                    event.get("event_source"),
                ))
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as ex:
                print(f"Erreur upsert event {event.get('event_id')}: {ex}")
                conn.rollback()

        conn.commit()
        cur.close()
        print(f"Upsert : {inserted} insérés, {skipped} ignorés")
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)