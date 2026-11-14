"""Point d'entrée pour l'ingestion."""
import argparse
from datetime import datetime, timedelta
from loguru import logger
from ingestion.api_client import ChicagoAPIClient
from ingestion.extractor import save_raw
from ingestion.config import config


def run(start_date: str | None = None, end_date: str | None = None):
    """
    Lance l'ingestion.
    
    Si start_date et end_date ne sont pas fournis :
    → ingestion des dernières 24h (hier → aujourd'hui)
    """
    # Si pas de dates → on prend hier → aujourd'hui
    if not start_date:
        end = datetime.utcnow()
        start = end - timedelta(days=1)
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")

    logger.info(f"   Ingestion {start_date} → {end_date}")
    logger.info(f"   API URL : {config.api_base_url}")
    logger.info(f"   Token   : {'OK' if config.app_token else ' No token'}")
    logger.info(f"   PageSize: {config.page_size:,}")

    # Instancie le client API
    client = ChicagoAPIClient(
        base_url=config.api_base_url,
        app_token=config.app_token,
    )

    # Fetch toutes les données de la période
    records = client.fetch_incremental(
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(f" Total fetched: {len(records):,} records")

    # Date d'ingestion = aujourd'hui (pour le partitionnement)
    ingestion_date = start_date
    
    # Sauvegarde en RAW
    save_raw(records=records, ingestion_date=ingestion_date)


if __name__ == "__main__":
    # Parse les arguments de la ligne de commande
    parser = argparse.ArgumentParser(
        description="Chicago Crime Ingestion"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Date de début (YYYY-MM-DD). Défaut: hier."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Date de fin (YYYY-MM-DD). Défaut: aujourd'hui."
    )
    args = parser.parse_args()

    run(start_date=args.start_date, end_date=args.end_date)