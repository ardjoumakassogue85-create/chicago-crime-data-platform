"""Tests d'unicité."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger


def check_unique_id(df: DataFrame) -> dict:
    """
    Vérifie que la colonne 'id' est unique.
    
    Returns:
        dict avec :
        - check: nom du test
        - total_rows: total de lignes
        - distinct_ids: nombre d'IDs distincts
        - duplicates: nombre de doublons
        - passed: True si 0 doublons
    """
    total = df.count()
    distinct = df.select("id").distinct().count()
    duplicates = total - distinct
    
    result = {
        "check": "unique_id",
        "total_rows": total,
        "distinct_ids": distinct,
        "duplicates": duplicates,
        "passed": duplicates == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} unique_id : {duplicates} doublons sur {total:,}")
    
    return result