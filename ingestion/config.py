"""Configuration centrale de l'ingestion."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Charge le fichier .env
load_dotenv()


@dataclass(frozen=True)
class Config:
    """Configuration immuable (frozen=True)."""
    
    # API Socrata
    api_base_url: str = os.getenv("CHICAGO_API_BASE_URL")
    # Token Socrata (augmente les limites API)
    app_token: str = os.getenv("CHICAGO_APP_TOKEN", "")
    
    # Pagination : Socrata accepte max 50 000 par requête
    page_size: int = 50_000
    
    # Retry : 5 tentatives max
    max_retries: int = 5
    # Backoff exponentiel : 2s, 4s, 8s, 16s, 32s
    retry_backoff: float = 2.0
    # Timeout HTTP (60 secondes)
    timeout: int = 60
    
    # Chemin de sauvegarde local
    raw_path: str = "data/raw/crimes"
    
    # GCP (sera utilisé plus tard)
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcs_bucket_name: str = os.getenv("GCP_BUCKET_NAME", "")
    gcp_region: str = os.getenv("GCP_REGION", "europe-west1")
    gcp_sa_key_path: str = os.getenv("GCP_SA_KEY_PATH", "")


# Singleton de configuration
config = Config()