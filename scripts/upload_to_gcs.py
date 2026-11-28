"""Upload les fichiers JSON raw locaux vers GCS.

Usage :
    python -m scripts.upload_to_gcs 2024-01-01   → envoie uniquement cette date (utilisé par le DAG et le backfill)
    python -m scripts.upload_to_gcs              → envoie toutes les dates présentes en local (comme avant)
"""
import os
import shutil
import sys
from pathlib import Path
from google.cloud import storage
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def upload_raw_to_gcs(local_path: str, bucket_name: str, ingestion_date: str | None = None):
    """
    Upload les fichiers JSON de data/raw/crimes/ vers GCS.

    Structure locale :
        data/raw/crimes/ingestion_date=2024-01-01/part-000000.json

    Structure GCS :
        gs://bucket/raw/crimes/ingestion_date=2024-01-01/part-000000.json

    Si ingestion_date est donné :
        - seule cette partition est envoyée
        - la partition GCS de cette date est vidée avant l'envoi
          (relancer = remplacer, jamais de doublons)
        - le dossier local est supprimé après l'envoi
          (évite de remplir le disque du conteneur pendant le backfill)
    """
    # Auth via la clé
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_SA_KEY_PATH")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    local_raw = Path(local_path)
    source = local_raw / f"ingestion_date={ingestion_date}" if ingestion_date else local_raw
    if not source.exists():
        raise FileNotFoundError(f"Local path not found: {source}")

    # Parcourt les fichiers JSON (d'une seule date, ou de toutes)
    json_files = sorted(source.rglob("part-*.json"))
    if not json_files:
        raise FileNotFoundError(f"Aucun fichier part-*.json dans {source}")
    logger.info(f"Found {len(json_files)} files to upload")

    # Vide la partition GCS de cette date avant d'envoyer la nouvelle version
    if ingestion_date:
        prefix = f"raw/{local_raw.name}/ingestion_date={ingestion_date}/"
        old_blobs = list(client.list_blobs(bucket_name, prefix=prefix))
        for blob in old_blobs:
            blob.delete()
        if old_blobs:
            logger.info(f"Deleted {len(old_blobs)} old files in gs://{bucket_name}/{prefix}")

    uploaded = 0
    for json_file in json_files:
        # ex: crimes/ingestion_date=2024-01-01/part-000000.json
        relative = json_file.relative_to(local_raw.parent)

        # Chemin sur GCS : raw/crimes/ingestion_date=.../part-....json
        gcs_path = f"raw/{relative.as_posix()}"

        bucket.blob(gcs_path).upload_from_filename(str(json_file))

        uploaded += 1
        logger.info(f"[{uploaded}/{len(json_files)}] {gcs_path}")

    logger.info(f"Uploaded {uploaded} files to gs://{bucket_name}/raw/")

    # Nettoie le dossier local de cette date
    if ingestion_date:
        shutil.rmtree(source)
        logger.info(f"Removed local folder {source}")


if __name__ == "__main__":
    from scripts.logging_config import setup_logging
    setup_logging()
    upload_raw_to_gcs(
        local_path="data/raw/crimes",
        bucket_name=os.getenv("GCP_BUCKET_NAME"),
        ingestion_date=sys.argv[1] if len(sys.argv) > 1 else None,
    )