import json
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = "labels-raw"

LABEL_FILES = [
    "sunset_records.json",
    "nightwave_music.json",
    "urban_pulse.json",
]


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "catalog"],
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio() -> list:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        catalogs = []

        for file_name in LABEL_FILES:
            print(f"Téléchargement de {file_name} depuis MinIO...")
            obj = s3.get_object(Bucket=MINIO_BUCKET, Key=file_name)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            catalogs.append(data)

        print(f"{len(catalogs)} catalogues chargés depuis MinIO.")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(catalogs: list) -> list:
        valid_catalogs = []

        for catalog in catalogs:
            valid_artists = []

            for artist in catalog.get("artists", []):
                if not all(k in artist for k in ["id", "name"]):
                    continue

                valid_albums = []

                for album in artist.get("albums", []):
                    if not all(k in album for k in ["id", "title"]):
                        continue

                    valid_tracks = []

                    for track in album.get("tracks", []):
                        if not all(k in track for k in ["id", "title", "duration_ms"]):
                            continue

                        if int(track.get("duration_ms", 0)) <= 0:
                            continue

                        valid_tracks.append(track)

                    album["tracks"] = valid_tracks
                    valid_albums.append(album)

                artist["albums"] = valid_albums
                valid_artists.append(artist)

            catalog["artists"] = valid_artists
            valid_catalogs.append(catalog)

        return valid_catalogs

    @task(task_id="transform_catalog")
    def transform_catalog(catalogs: list) -> dict:
        artists = []
        albums = []
        tracks = []

        for catalog in catalogs:
            label = catalog.get("label", "Unknown")

            for artist in catalog.get("artists", []):
                artist_id = artist["id"]

                artists.append({
                    "id": artist_id,
                    "name": artist["name"].strip().title(),
                    "country": artist.get("country"),
                    "label": artist.get("label", label),
                    "genres": artist.get("genres", []),
                    "monthly_listeners": artist.get("monthly_listeners", 0),
                })

                for album in artist.get("albums", []):
                    album_id = album["id"]

                    albums.append({
                        "id": album_id,
                        "artist_id": album.get("artist_id", artist_id),
                        "title": album["title"].strip(),
                        "release_year": album.get("release_year"),
                        "total_tracks": album.get("total_tracks", len(album.get("tracks", []))),
                    })

                    for track in album.get("tracks", []):
                        tracks.append({
                            "id": track["id"],
                            "album_id": track.get("album_id", album_id),
                            "artist_id": track.get("artist_id", artist_id),
                            "title": track["title"].strip(),
                            "duration_ms": int(track["duration_ms"]),
                            "genre": track.get("genre"),
                            "bpm": track.get("bpm"),
                            "explicit": track.get("explicit", False),
                            "audio_file_path": track.get("audio_file_path"),
                        })

        print(f"Artists transformés : {len(artists)}")
        print(f"Albums transformés : {len(albums)}")
        print(f"Tracks transformées : {len(tracks)}")

        return {
            "artists": artists,
            "albums": albums,
            "tracks": tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(data: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            for artist in data["artists"]:
                cur.execute(
                    """
                    INSERT INTO artists (
                        id, name, country, label, genres, monthly_listeners, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (name, label) DO UPDATE SET
                        country = EXCLUDED.country,
                        genres = EXCLUDED.genres,
                        monthly_listeners = EXCLUDED.monthly_listeners,
                        updated_at = NOW();
                    """,
                    (
                        artist["id"],
                        artist["name"],
                        artist["country"],
                        artist["label"],
                        artist["genres"],
                        artist["monthly_listeners"],
                    ),
                )

            for album in data["albums"]:
                cur.execute(
                    """
                    INSERT INTO albums (
                        id, artist_id, title, release_year, total_tracks
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        release_year = EXCLUDED.release_year,
                        total_tracks = EXCLUDED.total_tracks;
                    """,
                    (
                        album["id"],
                        album["artist_id"],
                        album["title"],
                        album["release_year"],
                        album["total_tracks"],
                    ),
                )

            for track in data["tracks"]:
                cur.execute(
                    """
                    INSERT INTO tracks (
                        id, album_id, artist_id, title, duration_ms,
                        genre, bpm, explicit, audio_file_path, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        duration_ms = EXCLUDED.duration_ms,
                        genre = EXCLUDED.genre,
                        bpm = EXCLUDED.bpm,
                        explicit = EXCLUDED.explicit,
                        audio_file_path = EXCLUDED.audio_file_path,
                        updated_at = NOW();
                    """,
                    (
                        track["id"],
                        track["album_id"],
                        track["artist_id"],
                        track["title"],
                        track["duration_ms"],
                        track["genre"],
                        track["bpm"],
                        track["explicit"],
                        track["audio_file_path"],
                    ),
                )

            conn.commit()

            stats = {
                "artists": len(data["artists"]),
                "albums": len(data["albums"]),
                "tracks": len(data["tracks"]),
            }

            print(f"Chargement terminé : {stats}")
            return stats

        except Exception as e:
            conn.rollback()
            print(f"Erreur pendant le chargement PostgreSQL : {e}")
            raise

        finally:
            cur.close()
            conn.close()

    @task(task_id="notify_success")
    def notify_success(stats: dict):
        print("catalog_ingestion_pipeline terminé avec succès")
        print(f"Artists insérés : {stats['artists']}")
        print(f"Albums insérés : {stats['albums']}")
        print(f"Tracks insérées : {stats['tracks']}")

    raw = extract_from_minio()
    valid = validate_schema(raw)
    transformed = transform_catalog(valid)
    stats = load_to_postgres(transformed)
    notify_success(stats)