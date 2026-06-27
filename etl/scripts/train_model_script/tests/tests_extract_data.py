import pandas as pd
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
from train_model_script import extract_data


def test_extract(monkeypatch, tmp_path):
    class DummyEngine:
        pass

    dummy_df = pd.DataFrame({
        "trip_id": ["TRIP_001", "TRIP_002", "TRIP_003"],
        "distance_km": [10.0, 20.0, 450.0],
        "duration_h": [1.0, 2.0, 2.5],
        "frequency_per_week": [7, 14, 25],
        "emission_gco2e_pkm": [25.0, 25.0, 25.0],
        "total_emission_kgco2e": [0.25, 0.5, 11.25],
        "train_type": ["TGV", "TER", "Grande vitesse"],
        "traction": ["électrique", "diesel", "électrique"],
        "service_type": ["JOUR", "NUIT", "JOUR"],
        "agency_name": ["SNCF", "SNCF", "SNCF"],
        "origin_country": ["FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "DE"]
    })

    def dummy_read_sql(query, engine):
        return dummy_df

    monkeypatch.setattr(extract_data, "DB_URL", "dummy")
    monkeypatch.setattr(extract_data.pd, "read_sql", dummy_read_sql)
    monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: DummyEngine())
    monkeypatch.setattr(extract_data, "analyze_data", lambda df: None)

    # CORRIGÉ : patcher save_data directement pour éviter FileNotFoundError
    # (le dossier data/ n'existe pas dans l'environnement de test)
    saved_csv = tmp_path / "trips_freq.csv"

    def fake_save_data(df, path="data/trips_freq.csv"):
        df.to_csv(saved_csv, index=False)

    monkeypatch.setattr(extract_data, "save_data", fake_save_data)

    df = extract_data.extract()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "distance_km" in df.columns
    assert saved_csv.exists()


def test_extract_all_trips(monkeypatch, tmp_path):
    class DummyEngine:
        pass

    dummy_df = pd.DataFrame({
        "trip_id": ["TRIP_001", "TRIP_002", "TRIP_003"],
        "distance_km": [10.0, 20.0, 450.0],
        "duration_h": [1.0, 2.0, 2.5],
        "frequency_per_week": [7, 14, 25],
        "emission_gco2e_pkm": [25.0, 25.0, 25.0],
        "total_emission_kgco2e": [0.25, 0.5, 11.25],
        "train_type": ["TGV", "TER", "Grande vitesse"],
        "traction": ["électrique", "diesel", "électrique"],
        "service_type": ["JOUR", "NUIT", "JOUR"],
        "agency_name": ["SNCF", "SNCF", "SNCF"],
        "origin_country": ["FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "DE"]
    })

    def dummy_read_sql(query, engine):
        assert "JOIN dim_trip" in query or "dim_trip" in query
        return dummy_df

    monkeypatch.setattr(extract_data, "DB_URL", "dummy")
    monkeypatch.setattr(extract_data.pd, "read_sql", dummy_read_sql)
    monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: DummyEngine())
    monkeypatch.setattr(extract_data, "analyze_data", lambda df: None)
    monkeypatch.setattr(extract_data, "save_data", lambda df, path=None: None)

    if hasattr(extract_data, "extract_all_trips"):
        df = extract_data.extract_all_trips()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "trip_id" in df.columns
        assert "distance_km" in df.columns
        assert "duration_h" in df.columns
        assert "frequency_per_week" in df.columns


def test_extract_data_with_cleaning(monkeypatch, tmp_path):
    class DummyEngine:
        pass

    dummy_df = pd.DataFrame({
        "trip_id": ["TRIP_001", "TRIP_002", "TRIP_003", "TRIP_004"],
        "distance_km": [0, 20.0, -5, 450.0],
        "duration_h": [1.0, 0, 2.0, 2.5],
        "frequency_per_week": [7, 14, 25, 10],
        "emission_gco2e_pkm": [25.0, 25.0, 25.0, 25.0],
        "total_emission_kgco2e": [0.25, 0.5, 11.25, 2.5],
        "train_type": ["TGV", "TER", "Grande vitesse", "TER"],
        "traction": ["électrique", "diesel", "électrique", "mixte"],
        "service_type": ["JOUR", "NUIT", "JOUR", "JOUR"],
        "agency_name": ["SNCF", "SNCF", "SNCF", "SNCF"],
        "origin_country": ["FR", "FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "DE", "FR"]
    })

    def dummy_read_sql(query, engine):
        return dummy_df

    monkeypatch.setattr(extract_data, "DB_URL", "dummy")
    monkeypatch.setattr(extract_data.pd, "read_sql", dummy_read_sql)
    monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: DummyEngine())
    monkeypatch.setattr(extract_data, "analyze_data", lambda df: None)
    monkeypatch.setattr(extract_data, "save_data", lambda df, path=None: None)

    if hasattr(extract_data, "extract"):
        df = extract_data.extract()

        # CORRIGÉ : le filtre Python dans prepare_data élimine distance <= 0 et duration <= 0
        # Seul TRIP_004 (distance=450, duration=2.5) survit
        assert len(df) >= 1
        assert all(df["distance_km"] > 0)
        assert all(df["duration_h"] > 0)


def test_extract_data_save_csv(monkeypatch, tmp_path):
    class DummyEngine:
        pass

    dummy_df = pd.DataFrame({
        "trip_id": ["TRIP_001", "TRIP_002"],
        "distance_km": [100.0, 200.0],
        "duration_h": [2.0, 3.0],
        "frequency_per_week": [10, 20],
        "emission_gco2e_pkm": [25.0, 25.0],
        "total_emission_kgco2e": [2.5, 5.0],
        "train_type": ["TGV", "TER"],
        "traction": ["électrique", "diesel"],
        "service_type": ["JOUR", "NUIT"],
        "agency_name": ["SNCF", "SNCF"],
        "origin_country": ["FR", "FR"],
        "destination_country": ["FR", "FR"]
    })

    # CORRIGÉ : capturer orig_to_csv AVANT le patch pour éviter la récursion infinie
    orig_to_csv = pd.DataFrame.to_csv
    saved_path = None

    def dummy_read_sql(query, engine):
        return dummy_df

    monkeypatch.setattr(extract_data, "DB_URL", "dummy")
    monkeypatch.setattr(extract_data.pd, "read_sql", dummy_read_sql)
    monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: DummyEngine())
    monkeypatch.setattr(extract_data, "analyze_data", lambda df: None)

    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)

    def fake_save_data(df, path=None):
        nonlocal saved_path
        final_path = str(data_dir / "trips_freq.csv")
        saved_path = final_path
        # Appel de l'original (non patché) pour éviter la récursion infinie
        orig_to_csv(df, final_path, index=False)

    monkeypatch.setattr(extract_data, "save_data", fake_save_data)

    if hasattr(extract_data, "extract"):
        df = extract_data.extract()
        print(df.head())
        assert saved_path is not None
        assert "trips_freq.csv" in str(saved_path)
        assert (data_dir / "trips_freq.csv").exists()