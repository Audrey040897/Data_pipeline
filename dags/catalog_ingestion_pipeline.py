import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """Télécharge les fichiers JSON des labels depuis MinIO."""
        # Connexion à MinIO via boto3 (config par défaut du prof)
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        catalogues = []
        
        for file_name in LABEL_FILES:
            try:
                print(f"Téléchargement de {file_name} depuis le bucket {MINIO_BUCKET}...")
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=file_name)
                data = json.loads(obj['Body'].read().decode('utf-8'))
                catalogues.append(data)
            except s3.exceptions.NoSuchKey:
                # Gestion demandée par le prof : Warning et on continue
                print(f"⚠️ ATTENTION : Le fichier {file_name} est manquant dans MinIO. Passage au suivant.")
            except Exception as e:
                print(f"❌ Erreur lors de la lecture de {file_name}: {str(e)}")
                raise e
                
        return catalogues


    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """Valide le schéma de chaque catalogue et isole les entrées invalides en DLQ."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        valid_artists = []
        valid_albums = []
        valid_tracks = []
        errors_count = 0
        
        # Connexion pour insérer les erreurs au fil de l'eau dans dead_letter_events
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        dlq_query = """
            INSERT INTO dead_letter_events (error_type, payload, created_at)
            VALUES (%s, %s, NOW());
        """

        for catalog in raw_catalogs:
            # 1. Validation des Artistes
            for artist in catalog.get('artists', []):
                if all(k in artist for k in ('id', 'name', 'label')):
                    valid_artists.append(artist)
                else:
                    cursor.execute(dlq_query, ("schema_validation", json.dumps(artist)))
                    errors_count += 1

            # 2. Validation des Albums
            for album in catalog.get('albums', []):
                if all(k in album for k in ('id', 'artist_id', 'title')):
                    valid_albums.append(album)
                else:
                    cursor.execute(dlq_query, ("schema_validation", json.dumps(album)))
                    errors_count += 1

            # 3. Validation des Tracks
            for track in catalog.get('tracks', []):
                if all(k in track for k in ('id', 'artist_id', 'title', 'duration_ms')):
                    valid_tracks.append(track)
                else:
                    cursor.execute(dlq_query, ("schema_validation", json.dumps(track)))
                    errors_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "valid": {
                "artists": valid_artists,
                "albums": valid_albums,
                "tracks": valid_tracks
            },
            "errors_count": errors_count
        }


    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """Transforme, nettoie et normalise les données."""
        valid_data = validated["valid"]
        
        transformed_artists = {} # Utilisation d'un dict pour dédoublonner par (name, label)
        transformed_albums = []
        transformed_tracks = []
        
        # 1. Normalisation des Artistes (strip, title case, suppression doublons)
        for artist in valid_data["artists"]:
            clean_name = artist["name"].strip().title()
            clean_label = artist["label"].strip()
            key = (clean_name, clean_label)
            
            # En utilisant un dictionnaire, le dernier écrase le premier = dédoublonnage efficace
            transformed_artists[key] = {
                "id": artist["id"],
                "name": clean_name,
                "label": clean_label,
                "genres": artist.get("genres", []),
                "monthly_listeners": artist.get("monthly_listeners", 0)
            }

        # 2. Normalisation des Albums
        for album in valid_data["albums"]:
            transformed_albums.append({
                "id": album["id"],
                "artist_id": album["artist_id"],
                "title": album["title"].strip(),
                "release_year": album.get("release_year")
            })

        # 3. Validation et normalisation des Tracks (duration_ms > 0 et < 3_600_000)
        for track in valid_data["tracks"]:
            duration = track["duration_ms"]
            if 0 < duration < 3600000:
                transformed_tracks.append({
                    "id": track["id"],
                    "artist_id": track["artist_id"],
                    "title": track["title"].strip(),
                    "duration_ms": duration
                })
            else:
                print(f"⚠️ Track ignorée car durée invalide ({duration} ms) : {track.get('title')}")

        return {
            "artists": list(transformed_artists.values()),
            "albums": transformed_albums,
            "tracks": transformed_tracks
        }


    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """Charge les données dans PostgreSQL avec un upsert idempotent via executemany()."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Initialisation des compteurs de suivi
        stats = {
            "artists_inserted": len(transformed["artists"]),
            "albums_inserted": len(transformed["albums"]),
            "tracks_inserted": len(transformed["tracks"]),
            "errors_count": context['ti'].xcom_pull(task_ids='validate_schema')['errors_count']
        }

        try:
            # 1. Upsert ARTISTS (ON CONFLICT sur name, label)
            if transformed["artists"]:
                artist_query = """
                    INSERT INTO artists (id, name, label, genres, monthly_listeners)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name, label) DO UPDATE SET
                        monthly_listeners = EXCLUDED.monthly_listeners;
                """
                artist_tuples = [
                    (a["id"], a["name"], a["label"], a["genres"], a["monthly_listeners"]) 
                    for a in transformed["artists"]
                ]
                cursor.executemany(artist_query, artist_tuples)

            # 2. Upsert ALBUMS (ON CONFLICT sur id)
            if transformed["albums"]:
                album_query = """
                    INSERT INTO albums (id, artist_id, title, release_year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        release_year = EXCLUDED.release_year;
                """
                album_tuples = [
                    (al["id"], al["artist_id"], al["title"], al["release_year"]) 
                    for al in transformed["albums"]
                ]
                cursor.executemany(album_query, album_tuples)

            # 3. Upsert TRACKS (ON CONFLICT sur id)
            if transformed["tracks"]:
                track_query = """
                    INSERT INTO tracks (id, artist_id, title, duration_ms)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        duration_ms = EXCLUDED.duration_ms;
                """
                track_tuples = [
                    (t["id"], t["artist_id"], t["title"], t["duration_ms"]) 
                    for t in transformed["tracks"]
                ]
                cursor.executemany(track_query, track_tuples)

            conn.commit()
            print("🚀 Upsert en lot effectué avec succès dans PostgreSQL.")
            
        except Exception as e:
            conn.rollback()
            print("❌ Erreur durant le chargement SQL, rollback appliqué.")
            raise e
        finally:
            cursor.close()
            conn.close()

        # Envoi des statistiques clés dans XCom pour le monitoring de l'UI d'Airflow
        ti = context['ti']
        ti.xcom_push(key='tracks_inserted', value=stats["tracks_inserted"])
        ti.xcom_push(key='errors_count', value=stats["errors_count"])

        return stats


    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """Log de succès avec statistiques d'ingestion."""
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)