import json
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import redis

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline
Pipeline complet : Redis -> Validation -> Enrichissement -> Postgres/MinIO.
Gestion robuste des valeurs par défaut pour les contraintes NOT NULL.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
POSTGRES_CONN_ID = "spotify_postgres"
BATCH_SIZE = 1000

with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis() -> dict:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        listening, p2p = [], []
        for _ in range(BATCH_SIZE):
            msg = r.rpop("queue:listening_events")
            if not msg: break
            try: listening.append(json.loads(msg))
            except: pass
        for _ in range(BATCH_SIZE):
            msg = r.rpop("queue:p2p_network_events")
            if not msg: break
            try: p2p.append(json.loads(msg))
            except: pass
        return {"listening": listening, "p2p_network": p2p}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict) -> dict:
        REQUIRED = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
        valid_listening, valid_p2p, errors = [], [], []
        for event in raw_events.get("listening", []):
            if any(f not in event for f in REQUIRED):
                errors.append({**event, "error_type": "missing_fields"})
            else:
                valid_listening.append(event)
        for event in raw_events.get("p2p_network", []):
            if "event_id" not in event: errors.append({**event, "error_type": "p2p_invalid"})
            else: valid_p2p.append(event)
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict) -> list:
        listening_events = validated.get("valid_listening", [])
        if not listening_events: return []
        track_ids = list({e["track_id"] for e in listening_events if e.get("track_id")})
        catalog = {}
        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()
            # Requête corrigée : sans artist_name pour éviter l'erreur de colonne
            cur.execute("SELECT id, title FROM tracks WHERE id = ANY(%s::uuid[])", (track_ids,))
            rows = cur.fetchall()
            catalog = {row[0]: {"track_title": row[1]} for row in rows}
            cur.close(); conn.close()
        except Exception as ex:
            print(f"Erreur DB enrichissement : {ex}")
        return [{**e, **catalog.get(e["track_id"], {"track_title": "Titre Inconnu"})} for e in listening_events]

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_listening: list):
        if not enriched_listening: return
        df = pd.DataFrame(enriched_listening)
        run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        path = f"/tmp/data-{run_id}.parquet"
        df.to_parquet(path)
        s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
        try: 
            s3.upload_file(path, "spotify-parquet", f"listening/{run_id}.parquet")
        finally: 
            if os.path.exists(path): os.remove(path)

    @task(task_id="upsert_listening_to_postgres")
    def upsert_listening_to_postgres(enriched_listening: list):
        if not enriched_listening: return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # UUID fixe pour les artistes inconnus
        DEFAULT_ARTIST_ID = '00000000-0000-0000-0000-000000000000'
        
        for event in enriched_listening:
            try:
                # Maintenant ça fonctionnera car l'ID existe dans la table 'artists'
                cur.execute("""
                    INSERT INTO tracks (id, title, artist_id, duration_ms) 
                    VALUES (%s::uuid, %s, %s::uuid, %s) 
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event.get("track_id"), 
                    event.get("track_title", "Titre Inconnu"),
                    event.get("artist_id", DEFAULT_ARTIST_ID), 
                    event.get("duration_ms", 0)
                ))
                
                cur.execute("""
                    INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms)
                    VALUES (%s::uuid, %s, %s::uuid, %s, %s) 
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event.get("event_id"), 
                    event.get("user_id"), 
                    event.get("track_id"), 
                    event.get("timestamp"), 
                    event.get("duration_ms", 0)
                ))
                conn.commit()
            except Exception as ex:
                conn.rollback()
                print(f"Erreur sur {event.get('event_id')}: {ex}")
        cur.close(); conn.close()

    @task(task_id="upsert_p2p_to_postgres")
    def upsert_p2p_to_postgres(validated_events: dict):
        p2p_events = validated_events.get("valid_p2p", [])
        if not p2p_events: return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        for event in p2p_events:
            try:
                cur.execute("INSERT INTO p2p_network_events (id, timestamp) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", 
                            (event.get("event_id"), event.get("timestamp")))
                conn.commit()
            except Exception as ex:
                conn.rollback()
                print(f"Erreur P2P sur {event.get('event_id')}: {ex}")
        cur.close(); conn.close()

    raw_data = consume_from_redis()
    valid_data = validate_events(raw_data)
    enriched = enrich_events(valid_data)
    store_to_parquet(enriched)
    upsert_listening_to_postgres(enriched)
    upsert_p2p_to_postgres(valid_data)