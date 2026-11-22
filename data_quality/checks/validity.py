"""Tests de validité (plages, formats)."""
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger


def check_latitude_range(df: DataFrame) -> dict:
    """Vérifie que latitude est dans la plage de Chicago (40-43)."""
    total = df.count()
    invalid = df.filter(
        (F.col("latitude").isNotNull()) &
        (~F.col("latitude").between(40.0, 43.0))
    ).count()
    
    result = {
        "check": "latitude_range",
        "total_rows": total,
        "invalid_rows": invalid,
        "passed": invalid == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} latitude_range : {invalid} invalides")
    
    return result


def check_longitude_range(df: DataFrame) -> dict:
    """Vérifie que longitude est dans la plage de Chicago (-89 à -87)."""
    total = df.count()
    invalid = df.filter(
        (F.col("longitude").isNotNull()) &
        (~F.col("longitude").between(-89.0, -87.0))
    ).count()
    
    result = {
        "check": "longitude_range",
        "total_rows": total,
        "invalid_rows": invalid,
        "passed": invalid == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} longitude_range : {invalid} invalides")
    
    return result


def check_year_range(df: DataFrame) -> dict:
    """Vérifie que crime_year est entre 2001 et l'année courante."""
    current_year = datetime.utcnow().year
    total = df.count()
    invalid = df.filter(
        (F.col("crime_year").isNotNull()) &
        (~F.col("crime_year").between(2001, current_year + 1))
    ).count()
    
    result = {
        "check": "year_range",
        "total_rows": total,
        "invalid_rows": invalid,
        "passed": invalid == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} year_range : {invalid} invalides")
    
    return result


def check_arrest_boolean(df: DataFrame) -> dict:
    """Vérifie que arrest est bien un booléen (true/false)."""
    total = df.count()
    invalid = df.filter(
        F.col("arrest").isNotNull() &
        (~F.col("arrest").isin([True, False]))
    ).count()
    
    result = {
        "check": "arrest_boolean",
        "total_rows": total,
        "invalid_rows": invalid,
        "passed": invalid == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} arrest_boolean : {invalid} invalides")
    
    return result


def check_domestic_boolean(df: DataFrame) -> dict:
    """Vérifie que domestic est bien un booléen."""
    total = df.count()
    invalid = df.filter(
        F.col("domestic").isNotNull() &
        (~F.col("domestic").isin([True, False]))
    ).count()
    
    result = {
        "check": "domestic_boolean",
        "total_rows": total,
        "invalid_rows": invalid,
        "passed": invalid == 0,
    }
    
    status = "✅" if result["passed"] else "❌"
    logger.info(f"{status} domestic_boolean : {invalid} invalides")
    
    return result