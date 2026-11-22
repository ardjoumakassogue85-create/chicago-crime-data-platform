"""Tests de la lecture RAW → Bronze (JSON Socrata → types Spark)."""
import json
from datetime import datetime

from spark.jobs.raw_to_bronze import read_raw


def test_read_raw_multiline_array_and_casts(spark, tmp_path):
    """Tableau JSON indenté + nombres entre guillemets → lignes correctement typées."""
    records = [
        {
            "id": "13712100", "case_number": "JJ104803",
            "date": "2024-01-01T00:00:00.000", "primary_type": "MOTOR VEHICLE THEFT",
            "arrest": False, "domestic": True,
            "beat": "1634", "district": "016", "ward": "31",
            "year": "2024", "updated_on": "2025-01-06T15:41:41.000",
            "latitude": "41.95", "longitude": "-87.72",
        },
        {   # Socrata omet les champs nuls : ici pas de coordonnées
            "id": "13723851", "case_number": "JJ119220",
            "date": "2024-01-01T00:00:00.000", "primary_type": "OTHER OFFENSE",
            "arrest": False, "domestic": False,
            "beat": "0533", "district": "005", "year": "2024",
        },
    ]
    path = tmp_path / "crimes.json"
    path.write_text(json.dumps(records, indent=4))   # indenté, comme l'ingestion

    df = read_raw(spark, str(path))

    # Le bug d'origine : une ligne de fichier = une ligne Spark (29 010 lignes de null)
    assert df.count() == 2

    by_id = {r.id: r for r in df.collect()}
    first = by_id[13712100]

    assert dict(df.dtypes)["id"] == "int"
    assert first.beat == 1634
    assert first.district == 16            # "016" → 16 (zéro initial perdu)
    assert first.year == 2024
    assert first.arrest is False
    assert first.domestic is True
    assert first.date == datetime(2024, 1, 1, 0, 0)
    assert first.latitude == 41.95
    assert first._corrupt_record is None

    second = by_id[13723851]
    assert second.latitude is None         # champ absent → null
    assert second._corrupt_record is None