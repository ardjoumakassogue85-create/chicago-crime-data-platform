"""Génère un rapport de qualité des données."""
import sys
import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import DataFrame
from data_quality.checks.uniqueness import check_unique_id
from data_quality.checks.completeness import check_completeness
from data_quality.checks.validity import (
    check_latitude_range,
    check_longitude_range,
    check_year_range,
    check_arrest_boolean,
    check_domestic_boolean,
)


def run_all_checks(df: DataFrame) -> dict:
    """Exécute tous les tests et retourne un rapport."""
    checks = []
    
    # Unicité
    checks.append(check_unique_id(df))
    
    # Complétude
    checks.extend(check_completeness(df))
    
    # Validité
    checks.append(check_latitude_range(df))
    checks.append(check_longitude_range(df))
    checks.append(check_year_range(df))
    checks.append(check_arrest_boolean(df))
    checks.append(check_domestic_boolean(df))
    
    total = len(checks)
    passed = sum(1 for c in checks if c.get("passed", False))
    failed = total - passed
    
    quality_score = (passed / total) * 100 if total > 0 else 0
    
    return {
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "quality_score": round(quality_score, 2),
        "checks": checks,
    }


def print_report(report: dict):
    """Affiche le rapport de qualité."""
    print()
    print("=" * 60)
    print("📊 DATA QUALITY REPORT")
    print("=" * 60)
    print()
    
    for c in report["checks"]:
        status = "✅" if c.get("passed") else "❌"
        print(f"  {status} {c['check']}")
    
    print()
    print("-" * 60)
    print(f"  Quality Score : {report['quality_score']}%")
    print(f"  Passed         : {report['passed']}/{report['total_checks']}")
    print(f"  Failed         : {report['failed']}")
    print("=" * 60)
    print()


def main():
    """Point d'entrée : lit le Silver et lance les tests."""
    from spark.utils.spark_session import get_spark
    
    bucket = os.getenv("GCP_BUCKET_NAME")
    
    spark = get_spark("data_quality")
    
    # Lecture du Silver
    df = spark.read.parquet(f"gs://{bucket}/silver/crimes/")
    
    # Lancement des tests
    report = run_all_checks(df)
    print_report(report)
    
    spark.stop()


if __name__ == "__main__":
    main()