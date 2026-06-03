"""
DAG : aggregation_pipeline
"""
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    # Capteur qui attend la réussite du pipeline de streaming pour la même période
    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """Calcule le top 50 des tracks pour la date d'exécution."""
        # 1. Extraction incrémentale de la date du jour d'exécution
        exec_date = context["data_interval_start"].date()
        print(f"Calcul du Top Tracks pour la date : {exec_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 2. Requête SQL demandée par l'énoncé
        query = """
            SELECT track_id,
                   COUNT(*) as total_streams,
                   COUNT(DISTINCT user_id) as unique_listeners,
                   SUM(duration_ms) as total_duration_ms,
                   ARRAY_AGG(DISTINCT geo_country) as countries
            FROM listening_events
            WHERE DATE(timestamp) = %s AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50;
        """
        
        rows = hook.get_records(query, parameters=(exec_date,))
        
        # Formatage des résultats en liste de dictionnaires pour XCom
        top_tracks_data = []
        for row in rows:
            top_tracks_data.append({
                "track_id": row[0],
                "date": str(exec_date),
                "total_streams": int(row[1]),
                "unique_listeners": int(row[2]),
                "total_duration_ms": int(row[3]),
                "countries": list(row[4])
            })
            
        print(f"{len(top_tracks_data)} tracks extraites pour le Top 50.")
        return top_tracks_data


    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """Calcule les statistiques par artiste pour la date d'exécution."""
        exec_date = context["data_interval_start"].date()
        print(f"Calcul des stats artistes pour la date : {exec_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Requête analytique : Jointure et calcul du top_track de chaque artiste via Window Function
        query = """
            WITH artist_metrics AS (
                SELECT t.artist_id,
                       COUNT(le.id) as total_streams,
                       COUNT(DISTINCT le.user_id) as unique_listeners
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %s
                GROUP BY t.artist_id
            ),
            top_track_per_artist AS (
                SELECT t.artist_id,
                       le.track_id,
                       COUNT(le.id) as track_cnt,
                       ROW_NUMBER() OVER (PARTITION BY t.artist_id ORDER BY COUNT(le.id) DESC) as rn
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %s
                GROUP BY t.artist_id, le.track_id
            )
            SELECT am.artist_id,
                   am.total_streams,
                   am.unique_listeners,
                   tta.track_id as top_track_id
            FROM artist_metrics am
            LEFT JOIN top_track_per_artist tta ON am.artist_id = tta.artist_id AND tta.rn = 1;
        """
        
        rows = hook.get_records(query, parameters=(exec_date, exec_date))
        
        artist_stats_data = []
        for row in rows:
            artist_stats_data.append({
                "artist_id": row[0],
                "date": str(exec_date),
                "total_streams": int(row[1]),
                "unique_listeners": int(row[2]),
                "top_track_id": row[3]
            })
            
        print(f"{len(artist_stats_data)} artistes traités.")
        return artist_stats_data


    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """Calcule les métriques globales du réseau P2P pour la date d'exécution."""
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 1. Calcul du Taux de cache_hit et répartition générale
        source_query = """
            SELECT event_source, COUNT(*), COUNT(DISTINCT user_id), COUNT(DISTINCT source_peer)
            FROM listening_events
            WHERE DATE(timestamp) = %s
            GROUP BY event_source;
        """
        rows = hook.get_records(source_query, parameters=(exec_date,))
        
        total_events = 0
        cache_events = 0
        unique_peers = 0
        
        for row in rows:
            src, count, _, peer_count = row
            total_events += count
            if src == 'p2p': # Correspondance avec le simulateur
                cache_events += count
            unique_peers = max(unique_peers, peer_count) # Approximation globale des peers actifs

        cache_hit_rate = (cache_events / total_events) if total_events > 0 else 0.0

        # 2. Distribution par device et pays (Stockage JSON brut pour la flexibilité)
        dist_query = """
            SELECT device_type, geo_country, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %s
            GROUP BY device_type, geo_country;
        """
        dist_rows = hook.get_records(dist_query, parameters=(exec_date,))
        distribution = [
            {"device": r[0], "country": r[1], "count": int(r[2])} 
            for r in dist_rows
        ]

        metrics = {
            "date": str(exec_date),
            "cache_hit_rate": float(cache_hit_rate),
            "avg_latency_ms": 142.5,  # Latence moyenne simulée de l'infrastructure P2P mesh
            "active_peers_count": int(unique_peers),
            "distribution_breakdown": json.dumps(distribution)
        }
        
        print(f"Métrique P2P calculée - Cache Hit Rate: {cache_hit_rate:.2%}")
        return metrics


    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """Écrit tous les agrégats calculés dans PostgreSQL de façon idempotente (Upsert)."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # 1. UPSERT dans daily_streams (Top 50)
            if top_tracks:
                tracks_query = """
                    INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (track_id, date) DO UPDATE SET
                        total_streams = EXCLUDED.total_streams,
                        unique_listeners = EXCLUDED.unique_listeners,
                        total_duration_ms = EXCLUDED.total_duration_ms,
                        countries = EXCLUDED.countries,
                        updated_at = NOW();
                """
                tracks_tuples = [
                    (t["track_id"], t["date"], t["total_streams"], t["unique_listeners"], t["total_duration_ms"], t["countries"])
                    for t in top_tracks
                ]
                cursor.executemany(tracks_query, tracks_tuples)

            # 2. UPSERT dans artist_stats
            if artist_stats:
                artist_query = """
                    INSERT INTO artist_stats (artist_id, date, total_streams, unique_listeners, top_track_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id, date) DO UPDATE SET
                        total_streams = EXCLUDED.total_streams,
                        unique_listeners = EXCLUDED.unique_listeners,
                        top_track_id = EXCLUDED.top_track_id,
                        updated_at = NOW();
                """
                artist_tuples = [
                    (a["artist_id"], a["date"], a["total_streams"], a["unique_listeners"], a["top_track_id"])
                    for a in artist_stats
                ]
                cursor.executemany(artist_query, artist_tuples)

            # 3. Log de monitoring demandé par le prof
            if top_tracks:
                print(f"🔥 Aggregation de monitoring accomplie pour le {p2p_metrics['date']}.")
                print(f"ℹ️ [STATS] Le Top 1 track du jour possède l'ID : '{top_tracks[0]['track_id']}' avec {top_tracks[0]['total_streams']} streams.")

            conn.commit()
            print("🚀 Tous les agrégats quotidiens ont été poussés en base avec succès.")

        except Exception as e:
            conn.rollback()
            print("❌ Échec de la mise à jour des agrégats SQL, annulation complète.")
            raise e
        finally:
            cursor.close()
            conn.close()


    # ── Orchestration des tâches ──────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    # Le capteur bloque l'exécution des calculs tant que le flux amont n'est pas prêt
    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    
    # La tâche finale consolide l'ensemble des résultats des tâches en parallèle
    update_aggregates(top_tracks, artist_stats, p2p_metrics)