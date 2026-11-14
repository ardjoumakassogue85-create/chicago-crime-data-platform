"""Tests pour l'ingestion."""
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from ingestion.api_client import ChicagoAPIClient
from ingestion.extractor import save_raw, load_raw
from ingestion.config import Config


def test_save_raw_creates_partitioned_files(tmp_path, monkeypatch):
    """Vérifie que save_raw crée bien les fichiers partitionnés."""
    # Arrange : créer une nouvelle config avec le chemin temporaire
    temp_config = Config(raw_path=str(tmp_path / "raw"))
    monkeypatch.setattr("ingestion.config.config", temp_config)
    
    records = [
        {"id": 1, "primary_type": "THEFT"},
        {"id": 2, "primary_type": "BATTERY"},
    ]
    
    # Act
    base_dir = save_raw(records=records, ingestion_date="2024-01-01")
    
    # Assert
    assert base_dir.exists()
    assert "ingestion_date=2024-01-01" in str(base_dir)
    
    files = list(base_dir.glob("part-*.json"))
    assert len(files) == 1
    
    loaded = json.loads(files[0].read_text())
    assert len(loaded) == 2
    assert loaded[0]["id"] == 1


def test_save_raw_chunks_large_dataset(tmp_path, monkeypatch):
    """Vérifie que le chunking fonctionne."""
    temp_config = Config(raw_path=str(tmp_path / "raw"))
    monkeypatch.setattr("ingestion.config.config", temp_config)
    
    # 25 000 records → 3 chunks de 10 000
    records = [{"id": i} for i in range(25000)]
    
    base_dir = save_raw(records=records, ingestion_date="2024-01-02")
    
    files = sorted(base_dir.glob("part-*.json"))
    assert len(files) == 3
    
    # Premier fichier : 10 000
    first = json.loads(files[0].read_text())
    assert len(first) == 10000
    
    # Dernier fichier : 5 000
    last = json.loads(files[2].read_text())
    assert len(last) == 5000


@patch("ingestion.api_client.requests.Session")
def test_api_client_pagination(mock_session_cls):
    """Vérifie que la pagination fonctionne."""
    # Mock du session.get()
    session = MagicMock()
    mock_session_cls.return_value = session
    
    # Simule 2 pages puis une page vide
    responses = [
        MagicMock(json=lambda: [{"id": i} for i in range(50000)]),  # page 1: 50k
        MagicMock(json=lambda: [{"id": i} for i in range(30000)]),  # page 2: 30k (dernière)
    ]
    session.get.side_effect = responses
    for r in responses:
        r.raise_for_status.return_value = None
    
    client = ChicagoAPIClient("http://fake", "token")
    records = client.fetch_incremental("2024-01-01", "2024-01-02")
    
    assert len(records) == 80000
    assert session.get.call_count == 2