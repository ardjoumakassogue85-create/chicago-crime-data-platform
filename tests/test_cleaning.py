"""Tests pour le nettoyage Spark (Bronze → Silver)."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from spark.utils.cleaning import clean_crimes
from spark.utils.schema import bronze_schema

# Schéma de test = bronze_schema, mais tout nullable.
# Le JSON lu par Spark est toujours nullable, et createDataFrame refuse
# un None dans un champ déclaré nullable=False (id, date).
TEST_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in bronze_schema.fields]
)

# Ligne Bronze valide : les colonnes non listées (":id", "location"...) valent None
BASE_ROW = {
    "id": 1,
    "case_number": "HZ1",
    "date": datetime(2024, 1, 3, 14, 30),   # un mercredi
    "block": "0XX N MAIN ST",
    "iucr": "0486",
    "primary_type": "BATTERY",
    "description": "SIMPLE",
    "location_description": "STREET",
    "arrest": False,
    "domestic": True,
    "beat": 123,
    "district": 12,
    "ward": 27,
    "community_area": 24,
    "fbi_code": "08B",
    "year": 2024,
    "latitude": 41.88,
    "longitude": -87.63,
}


def make_bronze(spark, *overrides):
    """Construit un DataFrame Bronze : une ligne par dict d'overrides."""
    rows = [{**BASE_ROW, **o} for o in overrides]
    data = [tuple(r.get(f.name) for f in TEST_SCHEMA.fields) for r in rows]
    return spark.createDataFrame(data, schema=TEST_SCHEMA)


def test_id_null_filtered(spark):
    """Les lignes avec id=null doivent être filtrées par clean_crimes."""
    df = make_bronze(spark, {"id": None}, {"id": 1})
    result = clean_crimes(df)
    assert [r.id for r in result.collect()] == [1]


def test_duplicates_removed(spark):
    """Deux lignes avec le même id → une seule en sortie."""
    df = make_bronze(spark, {"id": 1}, {"id": 1}, {"id": 2})
    result = clean_crimes(df)
    assert result.count() == 2
    assert result.select("id").distinct().count() == 2


def test_invalid_coordinates_nullified(spark):
    """Les coordonnées hors Chicago deviennent null, les valides restent."""
    df = make_bronze(
        spark,
        {"id": 1, "latitude": 41.88, "longitude": -87.63},
        {"id": 2, "latitude": 50.0, "longitude": -100.0},
    )
    cleaned = clean_crimes(df)

    invalid = cleaned.filter(F.col("id") == 2).collect()[0]
    assert invalid.latitude is None
    assert invalid.longitude is None

    valid = cleaned.filter(F.col("id") == 1).collect()[0]
    assert valid.latitude == 41.88
    assert valid.longitude == -87.63


def test_date_renamed_and_temporal_columns(spark):
    """'date' devient 'crime_datetime' et les colonnes temporelles sont calculées."""
    cleaned = clean_crimes(make_bronze(spark, {"id": 1}))
    row = cleaned.collect()[0]

    assert "date" not in cleaned.columns
    assert "crime_datetime" in cleaned.columns

    assert row.crime_year == 2024
    assert row.crime_month == 1
    assert row.crime_day == 3
    assert row.crime_hour == 14
    assert row.crime_day_of_week == 4      # mercredi (dimanche = 1)
    assert row.crime_is_weekend == 0


@pytest.mark.parametrize(
    "crime_date, expected",
    [
        (datetime(2024, 1, 6, 10, 0), 1),   # samedi
        (datetime(2024, 1, 7, 10, 0), 1),   # dimanche
        (datetime(2024, 1, 8, 10, 0), 0),   # lundi
    ],
    ids=["samedi", "dimanche", "lundi"],
)
def test_weekend_flag(spark, crime_date, expected):
    cleaned = clean_crimes(make_bronze(spark, {"id": 1, "date": crime_date}))
    assert cleaned.collect()[0].crime_is_weekend == expected


def test_text_normalized(spark):
    """Texte : TRIM + UPPER, et 'UNKNOWN' si null."""
    df = make_bronze(
        spark,
        {
            "id": 1,
            "primary_type": "  battery ",
            "description": None,
            "location_description": "street",
        },
    )
    row = clean_crimes(df).collect()[0]

    assert row.primary_type == "BATTERY"
    assert row.description == "UNKNOWN"
    assert row.location_description == "STREET"


def test_technical_columns_dropped(spark):
    """Les colonnes techniques Socrata doivent être supprimées."""
    cols = clean_crimes(make_bronze(spark, {"id": 1})).columns

    dropped = [
        ":id", ":version", ":created_at", ":updated_at",
        "x_coordinate", "y_coordinate", "location",
    ] + [c for c in TEST_SCHEMA.fieldNames() if c.startswith(":@computed_region_")]

    for name in dropped:
        assert name not in cols, f"{name} devrait être supprimée"