"""Sauvegarde des records bruts en JSON partitionné par date."""
import json
from pathlib import Path
from loguru import logger
from ingestion.config import config


def save_raw(records: list[dict], ingestion_date: str) -> Path:
    """
    Sauvegarde les records dans la zone RAW.
    
    Structure de sortie :
        data/raw/crimes/
            ingestion_date=2024-01-15/
                part-000000.json
                part-000001.json
                part-000002.json
    
    Pourquoi partitionner par date ?
    - Si on veut retraiter le 15/01, on n'a qu'à lire ce dossier
    - Spark peut faire du "partition pruning" : ne lire QUE les partitions nécessaires
    - C'est le pattern standard d'un Data Lake
    
    Pourquoi chunker en fichiers de 10 000 ?
    - Un fichier JSON de 8M lignes serait impossible à manipuler
    - 10 000 lignes = ~5-10 Mo, parfait pour Spark
    """
    
    # Crée le dossier de partition
    base_dir = Path(config.raw_path) / f"ingestion_date={ingestion_date}"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Chunker : 10 000 records par fichier
    chunk_size = 10_000
    total_files = 0
    
    for i, start in enumerate(range(0, len(records), chunk_size)):
        # Slice de records
        chunk = records[start:start + chunk_size]
        
        # Nom du fichier : part-000000.json, part-000001.json, ...
        file_path = base_dir / f"part-{i:06d}.json"
        
        # Écriture JSON (default=str pour gérer les dates/timestamps)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(chunk, f, default=str)
        
        total_files += 1
    
    logger.info(
        f"Saved {len(records):,} records in {total_files} files at {base_dir}"
    )
    return base_dir


def load_raw(ingestion_date: str) -> list[dict]:
    """Charge tous les records d'une date d'ingestion (pour tests/debug)."""
    base_dir = Path(config.raw_path) / f"ingestion_date={ingestion_date}"
    
    if not base_dir.exists():
        raise FileNotFoundError(f"No raw data for ingestion_date={ingestion_date}")
    
    all_records = []
    for json_file in sorted(base_dir.glob("part-*.json")):
        with open(json_file, "r", encoding="utf-8") as f:
            all_records.extend(json.load(f))
    
    logger.info(f"Loaded {len(all_records):,} records from {base_dir}")
    return all_records