import pandas as pd
import os
import sys
import pytest

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
from train_model_script import extract_data

def test_extract(monkeypatch, tmp_path):
    class DummyEngine:
        pass

    dummy_df = pd.DataFrame({
        "distance_km": [10, 20],
        "duration_h": [1, 2],
        "frequency_per_week": [7, 14],
        "train_type": ["TGV", "TER"],
        "traction": ["Électrique", "Diesel"],
        "service_type": ["JOUR", "NUIT"],
        "agency_name": ["SNCF", "SNCF"],
        "origin_country": ["FR", "FR"],
        "destination_country": ["FR", "FR"]
    })

    def dummy_read_sql(query, engine):
        return dummy_df
    monkeypatch.setattr(extract_data, "DB_URL", "dummy")
    monkeypatch.setattr(extract_data.pd, "read_sql", dummy_read_sql)
    monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: DummyEngine())
    monkeypatch.setattr(extract_data, "print", lambda *a, **k: None)  # Ignore prints

    orig_to_csv = pd.DataFrame.to_csv
    def fake_to_csv(self, path, *args, **kwargs):
        # Redirige vers le dossier temporaire
        assert str(tmp_path) in str(tmp_path / "trips_freq.csv")
        orig_to_csv(self, tmp_path / "trips_freq.csv", *args, **kwargs)
    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv)

    df = extract_data.extract()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "distance_km" in df.columns
    assert (tmp_path / "trips_freq.csv").exists()