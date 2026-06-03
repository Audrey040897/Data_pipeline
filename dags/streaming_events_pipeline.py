"""
DAG : streaming_events_pipeline
================================
Consomme les événements d'écoute depuis Redis, valide le schéma,
enrichit les données et les stocke dans PostgreSQL et MinIO.

Planification : toutes les 5 minutes
Catchup       : désactivé
"""

import json
import os
from datetime import datetime, timedelta
import logging

import boto3
import pandas as pd
import redis

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Pipeline complet de traitement des événements d'écoute en temps réel :
Redis → Validation → Enrichissement → PostgreSQL/MinIO

### Flux
1. consume_from_redis() — Récupère les événements depuis Redis
2. validate_events() — Valide le schéma, envoie les erreurs en DLQ
3. enrich_events() — Enrichit avec les données du catalogue
4. store_to_parquet() — Exporte en Parquet sur MinIO
5. upsert_to_postgres() — Stocke dans PostgreSQL

### Monitoring
- Événements consommés par batch
- Taux de validation (% valid events)
- Enrichissement depuis le catalogue (lookup)
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
POSTGRES_CONN_ID = "spotify_postgres"
BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# TÂCHE 1: CONSUME FROM REDIS
# ─────────────────────────────────────────────────────────────

def consume_from_redis(**context):
    """Récupère les événements depuis les files Redis."""
    logger.info("=" * 80)
    logger.info("TASK 1/5: CONSUME FROM REDIS")
    logger.info("=" * 80)
    
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        logger.error(f"❌ Redis connection error: {e}")
        return {"listening": [], "p2p_network": []}
    
    listening_events = []
    p2p_events = []
    
    # Consommer les événements d'écoute
    for _ in range(BATCH_SIZE):
        msg = r.rpop("queue:listening_events")
        if not msg:
            break
        try:
            listening_events.append(json.loads(msg))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in listening_events")
    
    # Consommer les événements P2P
    for _ in range(BATCH_SIZE):
        msg = r.rpop("queue:p2p_network_events")
        if not msg:
            break
        try:
            p2p_events.append(json.loads(msg))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in p2p_network_events")
    
    logger.info(f"✅ Consumed {len(listening_events)} listening events + {len(p2p_events)} P2P events")
    
    context['ti'].xcom_push(key='listening_events', value=listening_events)
    context['ti'].xcom_push(key='p2p_events', value=p2p_events)
    
    return {
        "listening": listening_events,
        "p2p_network": p2p_events
    }


# ─────────────────────────────────────────────────────────────
# TÂCHE 2: VALIDATE EVENTS
# ─────────────────────────────────────────────────────────────

def validate_events(**context):
    """Valide le schéma des événements."""
    logger.info("=" * 80)
    logger.info("TASK 2/5: VALIDATE EVENTS")
    logger.info("=" * 80)
    
    listening = context['ti'].xcom_pull(task_ids='consume_from_redis', key='listening_events') or []
    p2p = context['ti'].xcom_pull(task_ids='consume_from_redis', key='p2p_events') or []
    
    REQUIRED_LISTENING = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
    
    valid_listening = []
    valid_p2p = []
    errors_count = 0
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Valider les événements d'écoute
    for event in listening:
        missing = [f for f in REQUIRED_LISTENING if f not in event]
        
        if missing:
            logger.warning(f"❌ Listening event missing {missing}")
            # Envoyer en DLQ
            pg_hook.run("""
                INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, parameters=(
                'schema_validation_error',
                json.dumps(event),
                f"Missing fields: {missing}",
                'pending'
            ))
            errors_count += 1
        else:
            valid_listening.append(event)
    
    # Valider les événements P2P
    for event in p2p:
        if "event_id" not in event:
            logger.warning(f"❌ P2P event missing event_id")
            pg_hook.run("""
                INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, parameters=(
                'schema_validation_error',
                json.dumps(event),
                "Missing event_id",
                'pending'
            ))
            errors_count += 1
        else:
            valid_p2p.append(event)
    
    logger.info(f"✅ Validated: {len(valid_listening)} listening + {len(valid_p2p)} P2P events")
    logger.info(f"⚠️  Errors sent to DLQ: {errors_count}")
    
    context['ti'].xcom_push(key='valid_listening', value=valid_listening)
    context['ti'].xcom_push(key='valid_p2p', value=valid_p2p)
    
    return {
        "valid_listening": valid_listening,
        "valid_p2p": valid_p2p,
        "errors_count": errors_count
    }


# ─────────────────────────────────────────────────────────────
# TÂCHE 3: ENRICH EVENTS
# ─────────────────────────────────────────────────────────────

def enrich_events(**context):
    """Enrichit les événements avec les données du catalogue."""
    logger.info("=" * 80)
    logger.info("TASK 3/5: ENRICH EVENTS")
    logger.info("=" * 80)
    
    listening_events = context['ti'].xcom_pull(task_ids='validate_events', key='valid_listening') or []
    
    if not listening_events:
        logger.info("✅ No events to enrich")
        context['ti'].xcom_push(key='enriched_events', value=[])
        return []
    
    # Récupérer les IDs de tracks uniques
    track_ids = list({e.get("track_id") for e in listening_events if e.get("track_id")})
    
    catalog = {}
    
    if track_ids:
        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()
            
            # Récupérer les infos des tracks depuis PostgreSQL
            cur.execute("""
                SELECT id, title, artist_id 
                FROM tracks 
                WHERE id = ANY(%s::uuid[])
            """, (track_ids,))
            
            rows = cur.fetchall()
            catalog = {
                str(row[0]): {
                    "track_title": row[1],
                    "artist_id": str(row[2])
                } 
                for row in rows
            }
            
            cur.close()
            conn.close()
            
            logger.info(f"✅ Enriched {len(catalog)} tracks from catalog")
        
        except Exception as e:
            logger.error(f"⚠️  Error enriching from catalog: {e}")
    
    # Enrichir les événements
    enriched = []
    for event in listening_events:
        track_id = str(event.get("track_id", ""))
        enriched_event = {
            **event,
            **catalog.get(track_id, {"track_title": "Unknown Track", "artist_id": None})
        }
        enriched.append(enriched_event)
    
    logger.info(f"✅ Enriched {len(enriched)} events")
    context['ti'].xcom_push(key='enriched_events', value=enriched)
    
    return enriched


# ─────────────────────────────────────────────────────────────
# TÂCHE 4: STORE TO PARQUET
# ─────────────────────────────────────────────────────────────

def store_to_parquet(**context):
    """Exporte les événements enrichis en Parquet sur MinIO."""
    logger.info("=" * 80)
    logger.info("TASK 4/5: STORE TO PARQUET")
    logger.info("=" * 80)
    
    enriched_events = context['ti'].xcom_pull(task_ids='enrich_events', key='enriched_events') or []
    
    if not enriched_events:
        logger.info("✅ No events to export")
        return
    
    try:
        df = pd.DataFrame(enriched_events)
        
        run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        local_path = f"/tmp/data-{run_id}.parquet"
        
        df.to_parquet(local_path)
        logger.info(f"✅ Saved {len(df)} rows to {local_path}")
        
        # Upload to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        try:
            s3_client.upload_file(
                local_path,
                "spotify-parquet",
                f"listening_events/{run_id}.parquet"
            )
            logger.info(f"✅ Uploaded to s3://spotify-parquet/listening_events/{run_id}.parquet")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    
    except Exception as e:
        logger.error(f"❌ Error storing to Parquet: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# TÂCHE 5: UPSERT TO POSTGRES
# ─────────────────────────────────────────────────────────────

def upsert_to_postgres(**context):
    """Insère les événements dans PostgreSQL."""
    logger.info("=" * 80)
    logger.info("TASK 5/5: UPSERT TO POSTGRES")
    logger.info("=" * 80)
    
    enriched_events = context['ti'].xcom_pull(task_ids='enrich_events', key='enriched_events') or []
    
    if not enriched_events:
        logger.info("✅ No events to insert")
        return
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    
    inserted_count = 0
    
    for event in enriched_events:
        try:
            cur.execute("""
                INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                event.get("event_id"),
                event.get("user_id"),
                event.get("track_id"),
                event.get("timestamp"),
                event.get("duration_ms", 0)
            ))
            inserted_count += 1
            
        except Exception as e:
            logger.error(f"❌ Error inserting event {event.get('event_id')}: {e}")
            conn.rollback()
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"✅ Inserted {inserted_count} events into PostgreSQL")


# ─────────────────────────────────────────────────────────────
# CRÉER LE DAG ET LES OPÉRATEURS
# ─────────────────────────────────────────────────────────────

dag = DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "streaming", "events"],
    doc_md=DAG_DOC,
)

t1 = PythonOperator(
    task_id='consume_from_redis',
    python_callable=consume_from_redis,
    dag=dag
)

t2 = PythonOperator(
    task_id='validate_events',
    python_callable=validate_events,
    dag=dag
)

t3 = PythonOperator(
    task_id='enrich_events',
    python_callable=enrich_events,
    dag=dag
)

t4 = PythonOperator(
    task_id='store_to_parquet',
    python_callable=store_to_parquet,
    dag=dag
)

t5 = PythonOperator(
    task_id='upsert_to_postgres',
    python_callable=upsert_to_postgres,
    dag=dag
)

# Dépendances
t1 >> t2 >> t3 >> [t4, t5]