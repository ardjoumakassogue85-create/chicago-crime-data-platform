"""Upload les fichiers JSON raw locaux vers GCS."""
import os
from pathlib import Path
from google.cloud import storage
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def upload_raw_to_gcs(local_path: str, bucket_name: str):
    """
    Upload tous les fichiers JSON de data/raw/crimes/ vers GCS.
    
    Structure locale :
        data/raw/crimes/ingestion_date=2024-01-01/part-000000.json
    
    Structure GCS :
        gs://bucket/raw/crimes/ingestion_date=2024-01-01/part-000000.json
    """
    # Auth via la clé
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_SA_KEY_PATH")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    local_raw = Path(local_path)
    if not local_raw.exists():
        raise FileNotFoundError(f"Local path not found: {local_raw}")
    
    # Parcourt tous les fichiers JSON
    json_files = list(local_raw.rglob("part-*.json"))
    logger.info(f"Found {len(json_files)} files to upload")
    
    uploaded = 0
    for json_file in json_files:
        # Calcule le chemin relatif
        # ex: crimes/ingestion_date=2024-01-01/part-000000.json
        relative = json_file.relative_to(local_raw.parent)
        
        # Chemin sur GCS : raw/crimes/ingestion_date=.../part-....json
        gcs_path = f"raw/{relative}"
        
        # Upload
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(json_file))
        
        uploaded += 1
        logger.info(f"[{uploaded}/{len(json_files)}] {gcs_path}")
    
    logger.info(f"Uploaded {uploaded} files to gs://{bucket_name}/raw/")


if __name__ == "__main__":
    upload_raw_to_gcs(
        local_path="data/raw/crimes",
        bucket_name=os.getenv("GCP_BUCKET_NAME"),
    )