"""Tests de complétude (nullité)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger


# Colonnes OBLIGATOIRES (non-null)
REQUIRED_COLUMNS = [
    "id",
    "case_number",
    "crime_datetime",
    "primary_type",
    "arrest",
    "domestic",
    "crime_year",
]


def check_completeness(
    df: DataFrame,
    columns: list[str] = REQUIRED_COLUMNS
) -> list[dict]:
    """
    Vérifie le taux de nullité pour chaque colonne.
    
    Args:
        df: DataFrame Spark
        columns: Liste des colonnes à tester
        
    Returns:
        Liste de dicts, un par colonne
    """
    total = df.count()
    results = []
    
    for col in columns:
        # Compte les nulls
        null_count = df.filter(F.col(col).isNull()).count()
        null_rate = null_count / total if total > 0 else 1.0
        
        result = {
            "check": f"completeness_{col}",
            "column": col,
            "total_rows": total,
            "null_count": null_count,
            "null_rate": round(null_rate, 6),
            "passed": null_rate == 0.0,
        }
        results.append(result)
        
        status = "✅" if result["passed"] else "❌"
        logger.info(
            f"{status} completeness_{col} : "
            f"{null_count} nulls ({result['null_rate']*100:.4f}%)"
        )
    
    return results