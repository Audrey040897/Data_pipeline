import json
import os
"""
DAG : catalog_ingestion_pipeline
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()
"""

from datetime import datetime, timedelta
import json
import uuid
import logging
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_BUCKET = "labels-raw"
LABEL_FILES = ["sunset_records.json", "cosmic_beats.json", "urban_sounds.json"]

dag = DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion"],
)

# ─────────────────────────────────────────────────────────────
# TÂCHE 1: EXTRACT FROM MINIO
# ─────────────────────────────────────────────────────────────

def extract_from_minio(**context):
    """Télécharge les fichiers JSON des labels depuis MinIO."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("TASK 1/5: EXTRACT FROM MINIO")
    logger.info("=" * 80)
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )
    
    catalogs = []
    
    for file_name in LABEL_FILES:
        try:
            logger.info(f"↓ Downloading {file_name}...")
            response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=file_name)
            catalog = json.loads(response['Body'].read().decode('utf-8'))
            catalogs.append(catalog)
            
            artists_count = len(catalog.get('artists', []))
            logger.info(f"✅ {file_name}: {artists_count} artistes")
        
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"⚠️  File not found: {file_name}")
            else:
                logger.error(f"❌ S3 Error: {e}")
                raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {file_name}: {e}")
            raise
    
    logger.info(f"✅ Extraction complete: {len(catalogs)} catalogs")
    
    # Sauvegarder dans XCom
    context['ti'].xcom_push(key='catalogs', value=catalogs)
    return catalogs

# ─────────────────────────────────────────────────────────────
# TÂCHE 2: VALIDATE SCHEMA
# ─────────────────────────────────────────────────────────────

def validate_schema(**context):
    """Valide le schéma et envoie les invalides en DLQ."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("TASK 2/5: VALIDATE SCHEMA")
    logger.info("=" * 80)
    
    raw_catalogs = context['ti'].xcom_pull(task_ids='extract_from_minio', key='catalogs')
    
    REQUIRED_ARTIST_FIELDS = {'id', 'name', 'label'}
    REQUIRED_ALBUM_FIELDS = {'id', 'artist_id', 'title'}
    REQUIRED_TRACK_FIELDS = {'id', 'artist_id', 'title', 'duration_ms'}
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    valid_catalogs = []
    errors_count = 0
    
    for catalog in raw_catalogs:
        valid_catalog = {
            'label': catalog.get('label', 'Unknown'),
            'artists': []
        }
        
        for artist in catalog.get('artists', []):
            missing = REQUIRED_ARTIST_FIELDS - set(artist.keys())
            
            if missing:
                logger.warning(f"❌ Artist missing {missing}")
                
                pg_hook.run("""
                    INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, parameters=(
                    'schema_validation_error',
                    json.dumps(artist),
                    f"Missing {list(missing)}",
                    'pending'
                ))
                errors_count += 1
                continue
            
            valid_artist = {**artist, 'albums': []}
            
            for album in artist.get('albums', []):
                missing = REQUIRED_ALBUM_FIELDS - set(album.keys())
                
                if missing:
                    logger.warning(f"❌ Album missing {missing}")
                    pg_hook.run("""
                        INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, parameters=(
                        'schema_validation_error',
                        json.dumps(album),
                        f"Missing {list(missing)}",
                        'pending'
                    ))
                    errors_count += 1
                    continue
                
                valid_album = {**album, 'tracks': []}
                
                for track in album.get('tracks', []):
                    missing = REQUIRED_TRACK_FIELDS - set(track.keys())
                    
                    if missing:
                        logger.warning(f"❌ Track missing {missing}")
                        pg_hook.run("""
                            INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                            VALUES (%s, %s, %s, %s, NOW())
                        """, parameters=(
                            'schema_validation_error',
                            json.dumps(track),
                            f"Missing {list(missing)}",
                            'pending'
                        ))
                        errors_count += 1
                        continue
                    
                    duration = track.get('duration_ms', 0)
                    if not isinstance(duration, int) or duration <= 0 or duration > 3_600_000:
                        logger.warning(f"❌ Invalid duration: {duration}")
                        pg_hook.run("""
                            INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                            VALUES (%s, %s, %s, %s, NOW())
                        """, parameters=(
                            'schema_validation_error',
                            json.dumps(track),
                            f"Invalid duration {duration}",
                            'pending'
                        ))
                        errors_count += 1
                        continue
                    
                    valid_album['tracks'].append(track)
                
                valid_artist['albums'].append(valid_album)
            
            valid_catalog['artists'].append(valid_artist)
        
        valid_catalogs.append(valid_catalog)
        logger.info(f"✅ {catalog.get('label')}: valid")
    
    logger.info(f"✅ Validation complete: {errors_count} errors")
    
    context['ti'].xcom_push(key='valid_catalogs', value=valid_catalogs)
    context['ti'].xcom_push(key='errors_count', value=errors_count)
    return valid_catalogs

# ─────────────────────────────────────────────────────────────
# TÂCHE 3: TRANSFORM CATALOG
# ─────────────────────────────────────────────────────────────

def transform_catalog(**context):
    """Normalise les données du catalogue."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("TASK 3/5: TRANSFORM CATALOG")
    logger.info("=" * 80)
    
    valid_catalogs = context['ti'].xcom_pull(task_ids='validate_schema', key='valid_catalogs')
    
    VALID_GENRES = {
        'Rock', 'Pop', 'Jazz', 'Electronic', 'Hip-Hop', 'R&B', 'Country',
        'Classical', 'Folk', 'Metal', 'Punk', 'Soul', 'Reggae', 'Latin'
    }
    
    transformed_catalogs = []
    
    for catalog in valid_catalogs:
        transformed_catalog = {
            'label': catalog['label'].strip(),
            'artists': []
        }
        
        for artist in catalog['artists']:
            transformed_artist = {
                **artist,
                'name': artist['name'].strip().title(),
                'label': catalog['label'],
            }
            
            raw_genres = artist.get('genres', [])
            if not isinstance(raw_genres, list):
                raw_genres = [raw_genres]
            
            validated_genres = []
            for genre in raw_genres:
                g = genre.strip().title()
                if g in VALID_GENRES:
                    validated_genres.append(g)
                else:
                    logger.warning(f"⚠️  Unknown genre: {g}")
                    validated_genres.append('Unknown')
            
            transformed_artist['genres'] = validated_genres if validated_genres else ['Unknown']
            transformed_artist['albums'] = []
            
            for album in artist.get('albums', []):
                transformed_album = {
                    **album,
                    'title': album['title'].strip(),
                    'artist_id': artist['id'],
                    'tracks': []
                }
                
                for track in album.get('tracks', []):
                    transformed_track = {
                        **track,
                        'title': track['title'].strip(),
                        'artist_id': artist['id'],
                        'duration_ms': int(track['duration_ms'])
                    }
                    transformed_album['tracks'].append(transformed_track)
                
                transformed_artist['albums'].append(transformed_album)
            
            transformed_catalog['artists'].append(transformed_artist)
        
        transformed_catalogs.append(transformed_catalog)
        logger.info(f"✅ {catalog['label']}: transformed")
    
    context['ti'].xcom_push(key='transformed_catalogs', value=transformed_catalogs)
    return transformed_catalogs

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

def load_to_postgres(**context):
    """Charge dans PostgreSQL avec upsert idempotent."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("TASK 4/5: LOAD TO POSTGRES (UPSERT)")
    logger.info("=" * 80)
    
    transformed_catalogs = context['ti'].xcom_pull(task_ids='transform_catalog', key='transformed_catalogs')
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    stats = {
        'artists_inserted': 0,
        'albums_inserted': 0,
        'tracks_inserted': 0
    }
    
    try:
        for catalog in transformed_catalogs:
            logger.info(f"Loading {catalog['label']}")
            
            for artist in catalog['artists']:
                artist_id = artist.get('id') or str(uuid.uuid4())
                
                # UPSERT ARTIST
                pg_hook.run("""
                    INSERT INTO artists (id, name, label, genres, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (name, label) DO UPDATE SET
                        genres = EXCLUDED.genres,
                        updated_at = NOW()
                """, parameters=(
                    artist_id,
                    artist['name'],
                    catalog['label'],
                    artist['genres']
                ))
                
                stats['artists_inserted'] += 1
                
                # INSERT ALBUMS
                for album in artist.get('albums', []):
                    album_id = album.get('id') or str(uuid.uuid4())
                    
                    pg_hook.run("""
                        INSERT INTO albums (id, artist_id, title, created_at, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            updated_at = NOW()
                    """, parameters=(
                        album_id,
                        artist_id,
                        album['title']
                    ))
                    
                    stats['albums_inserted'] += 1
                    
                    # INSERT TRACKS
                    for track in album.get('tracks', []):
                        track_id = track.get('id') or str(uuid.uuid4())
                        
                        pg_hook.run("""
                            INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                title = EXCLUDED.title,
                                duration_ms = EXCLUDED.duration_ms,
                                updated_at = NOW()
                        """, parameters=(
                            track_id,
                            album_id,
                            artist_id,
                            track['title'],
                            track['duration_ms']
                        ))
                        
                        stats['tracks_inserted'] += 1
        
        logger.info(f"✅ Artists: {stats['artists_inserted']}")
        logger.info(f"✅ Albums: {stats['albums_inserted']}")
        logger.info(f"✅ Tracks: {stats['tracks_inserted']}")
        
        context['ti'].xcom_push(key='load_stats', value=stats)
        return stats
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise

# ─────────────────────────────────────────────────────────────
# TÂCHE 5: NOTIFY SUCCESS
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
def notify_success(**context):
    """Log de succès avec statistiques."""
    logger = logging.getLogger(__name__)
    
    stats = context['ti'].xcom_pull(task_ids='load_to_postgres', key='load_stats')
    errors = context['ti'].xcom_pull(task_ids='validate_schema', key='errors_count')
    
    logger.info("=" * 80)
    logger.info("✅ CATALOG INGESTION PIPELINE SUCCESS")
    logger.info("=" * 80)
    logger.info(f"Artists: {stats['artists_inserted']}")
    logger.info(f"Albums: {stats['albums_inserted']}")
    logger.info(f"Tracks: {stats['tracks_inserted']}")
    logger.info(f"Errors in DLQ: {errors}")
    logger.info("=" * 80)

# ─────────────────────────────────────────────────────────────
# CRÉER LES OPÉRATEURS
# ─────────────────────────────────────────────────────────────

t1 = PythonOperator(
    task_id='extract_from_minio',
    python_callable=extract_from_minio,
    dag=dag
)

t2 = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag
)

t3 = PythonOperator(
    task_id='transform_catalog',
    python_callable=transform_catalog,
    dag=dag
)

t4 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

t5 = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag
)

# ─────────────────────────────────────────────────────────────
# DÉPENDANCES
# ─────────────────────────────────────────────────────────────

t1 >> t2 >> t3 >> t4 >> t5
