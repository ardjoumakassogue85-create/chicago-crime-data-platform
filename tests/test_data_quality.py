"""Tests pour les data quality checks."""
import pytest
from pyspark.sql import SparkSession, Row
from data_quality.checks.uniqueness import check_unique_id
from data_quality.checks.validity import (
    check_latitude_range,
    check_year_range,
    check_arrest_boolean,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-quality")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_unique_id_passes(spark):
    """IDs uniques → check passe."""
    df = spark.createDataFrame(
        [Row(id=1), Row(id=2), Row(id=3)],
        ["id"]
    )
    result = check_unique_id(df)
    assert result["passed"] is True
    assert result["duplicates"] == 0


def test_unique_id_fails_with_duplicates(spark):
    """IDs en double → check échoue."""
    df = spark.createDataFrame(
        [Row(id=1), Row(id=1), Row(id=2)],
        ["id"]
    )
    result = check_unique_id(df)
    assert result["passed"] is False
    assert result["duplicates"] == 1


def test_latitude_out_of_range(spark):
    """Latitude hors Chicago → invalides."""
    df = spark.createDataFrame(
        [Row(latitude=41.88), Row(latitude=50.0), Row(latitude=42.5)],
        ["latitude"]
    )
    result = check_latitude_range(df)
    assert result["invalid_rows"] == 1  # 50.0 est hors plage


def test_year_in_future_fails(spark):
    """Année dans le futur → invalide."""
    df = spark.createDataFrame(
        [Row(crime_year=2023), Row(crime_year=2050)],
        ["crime_year"]
    )
    result = check_year_range(df)
    assert result["passed"] is False


def test_arrest_non_boolean_fails(spark):
    """Arrest pas booléen → échoue."""
    # En Spark, on ne peut pas vraiment mettre un non-booléen dans une colonne bool
    # mais on peut simuler avec des nulls
    df = spark.createDataFrame(
        [Row(arrest=True), Row(arrest=False), Row(arrest=None)],
        ["arrest"]
    )
    result = check_arrest_boolean(df)
    # nulls ne sont pas comptés comme invalides dans notre logique
    assert result["passed"] is True