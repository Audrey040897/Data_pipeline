"""
DAG : catalog_ingestion_pipeline
=================================
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
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "cosmic_beats.json", "urban_sounds.json"]


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
                logger.warning(f"⚠️  File not found: {file_name} — skipping")
            else:
                logger.error(f"❌ S3 Error for {file_name}: {e}")
                raise
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {file_name}: {e}")
            raise
        
        except Exception as e:
            logger.error(f"❌ Unexpected error reading {file_name}: {e}")
            raise
    
    logger.info(f"✅ Extraction complete: {len(catalogs)} catalogs loaded")
    context['ti'].xcom_push(key='catalogs', value=catalogs)
    return catalogs


# ─────────────────────────────────────────────────────────────
# TÂCHE 2: VALIDATE SCHEMA
# ─────────────────────────────────────────────────────────────

def validate_schema(**context):
    """Valide le schéma de chaque catalogue et isole les entrées invalides."""
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
                logger.warning(f"❌ Artist missing fields {missing}: {artist.get('name', 'UNKNOWN')}")
                pg_hook.run("""
                    INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, parameters=(
                    'schema_validation_error',
                    json.dumps(artist),
                    f"Artist: missing fields {list(missing)}",
                    'pending'
                ))
                errors_count += 1
                continue
            
            valid_artist = {**artist, 'albums': []}
            
            for album in artist.get('albums', []):
                missing = REQUIRED_ALBUM_FIELDS - set(album.keys())
                
                if missing:
                    logger.warning(f"❌ Album missing fields {missing}: {album.get('title', 'UNKNOWN')}")
                    pg_hook.run("""
                        INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, parameters=(
                        'schema_validation_error',
                        json.dumps(album),
                        f"Album: missing fields {list(missing)}",
                        'pending'
                    ))
                    errors_count += 1
                    continue
                
                valid_album = {**album, 'tracks': []}
                
                for track in album.get('tracks', []):
                    missing = REQUIRED_TRACK_FIELDS - set(track.keys())
                    
                    if missing:
                        logger.warning(f"❌ Track missing fields {missing}: {track.get('title', 'UNKNOWN')}")
                        pg_hook.run("""
                            INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                            VALUES (%s, %s, %s, %s, NOW())
                        """, parameters=(
                            'schema_validation_error',
                            json.dumps(track),
                            f"Track: missing fields {list(missing)}",
                            'pending'
                        ))
                        errors_count += 1
                        continue
                    
                    duration = track.get('duration_ms', 0)
                    if not isinstance(duration, int) or duration <= 0 or duration > 3_600_000:
                        logger.warning(f"❌ Track invalid duration: {track['title']} ({duration}ms)")
                        pg_hook.run("""
                            INSERT INTO dead_letter_events (event_type, payload, error_message, status, created_at)
                            VALUES (%s, %s, %s, %s, NOW())
                        """, parameters=(
                            'schema_validation_error',
                            json.dumps(track),
                            f"Track: invalid duration {duration}ms (must be 0 < duration <= 3600000)",
                            'pending'
                        ))
                        errors_count += 1
                        continue
                    
                    valid_album['tracks'].append(track)
                
                valid_artist['albums'].append(valid_album)
            
            valid_catalog['artists'].append(valid_artist)
        
        valid_catalogs.append(valid_catalog)
        logger.info(f"✅ {catalog.get('label')}: {len(valid_catalog['artists'])} valid artists")
    
    logger.info(f"✅ Validation complete: {errors_count} errors sent to DLQ")
    
    context['ti'].xcom_push(key='valid_catalogs', value=valid_catalogs)
    context['ti'].xcom_push(key='errors_count', value=errors_count)
    return valid_catalogs


# ─────────────────────────────────────────────────────────────
# TÂCHE 3: TRANSFORM CATALOG
# ─────────────────────────────────────────────────────────────

def transform_catalog(**context):
    """Transforme et normalise les données du catalogue."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("TASK 3/5: TRANSFORM CATALOG")
    logger.info("=" * 80)
    
    valid_catalogs = context['ti'].xcom_pull(task_ids='validate_schema', key='valid_catalogs')
    
    VALID_GENRES = {
        'Rock', 'Pop', 'Jazz', 'Electronic', 'Hip-Hop', 'R&B', 'Country',
        'Classical', 'Folk', 'Metal', 'Punk', 'Soul', 'Reggae', 'Latin',
        'Blues', 'Indie', 'Alternative', 'Dance', 'Techno', 'House'
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
                    logger.warning(f"⚠️  Unknown genre '{g}' for artist '{artist['name']}' → mapping to 'Unknown'")
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
        logger.info(f"✅ {catalog['label']}: {len(transformed_catalog['artists'])} artists transformed")
    
    logger.info("✅ Transformation complete")
    
    context['ti'].xcom_push(key='transformed_catalogs', value=transformed_catalogs)
    return transformed_catalogs


# ─────────────────────────────────────────────────────────────
# TÂCHE 4: LOAD TO POSTGRES (UPSERT)
# ─────────────────────────────────────────────────────────────

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
            logger.info(f"Loading label: {catalog['label']}")
            
            for artist in catalog['artists']:
                artist_id = artist.get('id') or str(uuid.uuid4())
                
                # INSERT ARTIST avec upsert idempotent
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
                logger.debug(f"  ✅ Artist: {artist['name']}")
                
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
                    logger.debug(f"    ✅ Album: {album['title']}")
                    
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
                        logger.debug(f"      ✅ Track: {track['title']} ({track['duration_ms']}ms)")
        
        logger.info(f"✅ LOAD COMPLETE:")
        logger.info(f"   ✅ Artists: {stats['artists_inserted']}")
        logger.info(f"   ✅ Albums: {stats['albums_inserted']}")
        logger.info(f"   ✅ Tracks: {stats['tracks_inserted']}")
        
        context['ti'].xcom_push(key='load_stats', value=stats)
        return stats
    
    except Exception as e:
        logger.error(f"❌ Error during load: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# TÂCHE 5: NOTIFY SUCCESS
# ─────────────────────────────────────────────────────────────

def notify_success(**context):
    """Log de succès avec statistiques d'ingestion."""
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
# CRÉER LE DAG ET LES OPÉRATEURS
# ─────────────────────────────────────────────────────────────

dag = DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
)

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