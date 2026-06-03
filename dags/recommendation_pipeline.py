"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

import json
import math
from collections import defaultdict

import redis

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
            SELECT
                user_id::text,
                track_id::text,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """

        rows = hook.get_records(sql)

        matrix = defaultdict(dict)

        for user_id, track_id, play_count in rows:
            matrix[user_id][track_id] = int(play_count)

        active_matrix = {
            user_id: tracks
            for user_id, tracks in matrix.items()
            if len(tracks) >= 3
        }

        print(f"{len(active_matrix)} utilisateurs actifs trouvés.")

        return {
            "matrix": dict(active_matrix),
            "users": list(active_matrix.keys())
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.
        Version custom sans scikit-learn.
        """

        matrix = matrix_data.get("matrix", {})
        users = matrix_data.get("users", [])

        if not matrix or len(users) < 2:
            print("Pas assez d'utilisateurs actifs pour générer des recommandations.")
            return {}

        all_tracks = sorted({
            track_id
            for user_tracks in matrix.values()
            for track_id in user_tracks.keys()
        })

        def build_vector(user_id: str) -> list:
            return [
                matrix[user_id].get(track_id, 0)
                for track_id in all_tracks
            ]

        def custom_cosine_similarity(vector_a: list, vector_b: list) -> float:
            dot_product = sum(a * b for a, b in zip(vector_a, vector_b))
            norm_a = math.sqrt(sum(a * a for a in vector_a))
            norm_b = math.sqrt(sum(b * b for b in vector_b))

            if norm_a == 0 or norm_b == 0:
                return 0.0

            return dot_product / (norm_a * norm_b)

        user_vectors = {
            user_id: build_vector(user_id)
            for user_id in users
        }

        recommendations = {}

        for user_id in users:
            listened_tracks = set(matrix[user_id].keys())
            similarities = []

            for other_user_id in users:
                if other_user_id == user_id:
                    continue

                similarity_score = custom_cosine_similarity(
                    user_vectors[user_id],
                    user_vectors[other_user_id]
                )

                similarities.append((other_user_id, similarity_score))

            similarities.sort(key=lambda item: item[1], reverse=True)

            top_neighbors = similarities[:5]
            candidate_scores = defaultdict(float)

            for neighbor_id, similarity_score in top_neighbors:
                if similarity_score <= 0:
                    continue

                for track_id, play_count in matrix[neighbor_id].items():
                    if track_id not in listened_tracks:
                        candidate_scores[track_id] += similarity_score * play_count

            top_recommendations = sorted(
                candidate_scores.items(),
                key=lambda item: item[1],
                reverse=True
            )[:TOP_N_RECO]

            recommendations[user_id] = [
                {
                    "track_id": track_id,
                    "score": round(score, 4)
                }
                for track_id, score in top_recommendations
            ]

        print(f"Recommandations générées pour {len(recommendations)} utilisateurs.")

        return recommendations
    
    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.
        """

        if not recommendations:
            return {
                "users_with_recos": 0,
                "total_recommendations": 0
            }

        redis_client = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows_to_insert = []
        total_recommendations = 0

        for user_id, recos in recommendations.items():

            track_ids = [
                reco["track_id"]
                for reco in recos
            ]

            redis_client.setex(
                f"reco:{user_id}",
                RECO_TTL_SECONDS,
                json.dumps(track_ids)
            )

            for reco in recos:
                rows_to_insert.append(
                    (
                        user_id,
                        reco["track_id"],
                        reco["score"]
                    )
                )

            total_recommendations += len(recos)

        if rows_to_insert:

            sql = """
                INSERT INTO recommendations
                (
                    user_id,
                    track_id,
                    score,
                    generated_at
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    NOW()
                )
                ON CONFLICT (user_id, track_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    generated_at = NOW()
            """

            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.executemany(
                sql,
                rows_to_insert
            )

            conn.commit()

            cursor.close()
            conn.close()

        print(
            f"{len(recommendations)} utilisateurs "
            f"avec recommandations."
        )

        print(
            f"{total_recommendations} recommandations stockées."
        )

        return {
            "users_with_recos": len(recommendations),
            "total_recommendations": total_recommendations
        }

    # ── Orchestration ─────────────────────────────────────────
    matrix        = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
