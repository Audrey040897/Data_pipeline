"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        sql = """
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """
        
        try:
            # Utilisation de get_records pour éviter une dépendance obligatoire à pandas
            records = hook.get_records(sql, parameters=(MAX_RETRIES, BATCH_SIZE))
            
            events = []
            for r in records:
                events.append({
                    "id": r[0],
                    "payload": r[1],
                    "error_type": r[2],
                    "retry_count": r[3],
                    "original_topic": r[4]
                })
                
            logging.info(f"{len(events)} événements 'pending' trouvés dans la DLQ.")
            return events
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des événements DLQ : {e}")
            raise e

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        """
        reprocessed = []
        failed = []
        
        for event in pending_events:
            event_id = event['id']
            payload = event['payload']
            
            # 1. Gestion du type de payload (doit être un dict)
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    logging.error(f"Event {event_id} : JSON invalide dans le payload.")
                    failed.append(event_id)
                    continue

            # 2. Validation : user_id est obligatoire
            if not payload.get('user_id'):
                logging.warning(f"Event {event_id} rejeté : user_id manquant.")
                failed.append(event_id)
                continue
            
            # 3. Validation : track_id est obligatoire
            if not payload.get('track_id'):
                logging.warning(f"Event {event_id} rejeté : track_id manquant.")
                failed.append(event_id)
                continue
                
            # 4. Correction : si le timestamp manque, on utilise le moment présent (idempotence relative)
            if not payload.get('timestamp'):
                payload['timestamp'] = datetime.now().isoformat()
                logging.info(f"Event {event_id} : timestamp corrigé.")

            reprocessed.append({
                "dlq_id": event_id,
                "payload": payload
            })
            
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events et injecte les succès.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])

        # 1. Traitement des événements réparés
        for item in reprocessed:
            dlq_id = item["dlq_id"]
            p = item["payload"]

            try:
                # Injection dans listening_events
                insert_sql = """
                    INSERT INTO listening_events (user_id, track_id, timestamp, device_type, geo_country)
                    VALUES (%s, %s, %s, %s, %s)
                """
                hook.run(insert_sql, parameters=(
                    p.get("user_id"), p.get("track_id"), p.get("timestamp"),
                    p.get("device_type"), p.get("geo_country")
                ))

                # Marquer comme traité dans la DLQ
                update_sql = "UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW() WHERE id=%s"
                hook.run(update_sql, parameters=(dlq_id,))
                
            except Exception as e:
                logging.error(f"Erreur lors de l'injection de l'event {dlq_id} : {e}")
                failed.append(dlq_id) # On le bascule en échec pour mise à jour du retry_count

        # 2. Traitement des échecs (incrémenter le retry_count ou abandonner)
        for dlq_id in failed:
            fail_sql = """
                UPDATE dead_letter_events
                SET retry_count = retry_count + 1,
                    last_retry_at = NOW(),
                    status = CASE WHEN retry_count + 1 >= %s THEN 'abandoned' ELSE 'pending' END
                WHERE id = %s
            """
            hook.run(fail_sql, parameters=(MAX_RETRIES, dlq_id))

        logging.info(f"Traitement terminé : {len(reprocessed)} retraités, {len(failed)} en échec/abandonnés.")
        return {"status": "success", "reprocessed": len(reprocessed), "failed": len(failed)}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
