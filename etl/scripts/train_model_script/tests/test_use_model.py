import joblib
import numpy as np
import pandas as pd
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
from train_model_script import use_model


class DummyModel:
    def predict(self, X):
        return np.ones(len(X))


def test_load_model(tmp_path):
    artifact = {"model": DummyModel(), "name": "RandomForest_Optimized"}
    model_path = tmp_path / "model.joblib"
    joblib.dump(artifact, model_path)
    model, name = use_model.load_model(path=str(model_path))
    assert name == "RandomForest_Optimized"
    assert hasattr(model, "predict")


def test_load_data(monkeypatch):
    df = pd.DataFrame({
        "distance_km": [10, 20, 450],
        "duration_h": [1, 2, 2.5],
        "frequency_per_week": [7, 14, 25],
        "train_type": ["TGV", "TER", "Grande vitesse"],
        "traction": ["électrique", "diesel", "électrique"],
        "service_type": ["JOUR", "NUIT", "JOUR"],
        "origin_country": ["FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "DE"]
    })
    monkeypatch.setattr(use_model.pd, "read_csv", lambda path: df)
    X, y = use_model.load_data()
    
    # Vérifie les nouvelles features
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert not X.empty
    assert not y.empty
    assert "speed_kmh" in X.columns
    assert "is_night" in X.columns
    assert "distance_night" in X.columns
    assert "service_type" in X.columns
    assert "traction" in X.columns


def test_evaluate():
    X = pd.DataFrame({
        "distance_km": [10, 20, 30],
        "duration_h": [1, 2, 3],
        "speed_kmh": [10, 10, 10],
        "is_night": [0, 0, 0],
        "distance_night": [0, 0, 0],
        "service_type": ["JOUR", "JOUR", "JOUR"],
        "traction": ["électrique", "électrique", "électrique"]
    })
    y = pd.Series([1, 1, 1])
    metrics = use_model.evaluate(DummyModel(), X, y)
    assert "r2" in metrics
    assert "mae" in metrics
    assert "mae_pct" in metrics
    assert "y_test" in metrics
    assert "y_pred" in metrics


def test_summary_model(monkeypatch, capsys):
    monkeypatch.setattr(use_model, "load_model", lambda: (DummyModel(), "RandomForest_Optimized"))
    
    df = pd.DataFrame({
        "distance_km": [10, 20, 30],
        "duration_h": [1, 2, 3],
        "frequency_per_week": [7, 14, 21],
        "train_type": ["TGV", "TER", "TGV"],
        "traction": ["électrique", "diesel", "électrique"],
        "service_type": ["JOUR", "NUIT", "JOUR"],
        "origin_country": ["FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "DE"]
    })
    
    def mock_load_data():
        df_copy = df.copy()
        df_copy['speed_kmh'] = df_copy['distance_km'] / df_copy['duration_h']
        df_copy['is_night'] = (df_copy['service_type'] == 'NUIT').astype(int)
        df_copy['distance_night'] = df_copy['distance_km'] * df_copy['is_night']
        X = df_copy[use_model.NUM_FEATURES + use_model.CAT_FEATURES]
        y = df_copy[use_model.TARGET]
        return X, y
    
    monkeypatch.setattr(use_model, "load_data", mock_load_data)
    metrics = use_model.summary_model()
    captured = capsys.readouterr()
    assert "Modèle" in captured.out
    assert isinstance(metrics, dict)
    assert "r2" in metrics


def test_manual_cases(capsys):
    use_model.manual_cases(DummyModel())
    captured = capsys.readouterr()
    assert "CAS MANUELS" in captured.out
    assert "TGV Paris→Lyon" in captured.out
    assert "TER court" in captured.out
    assert "Train de nuit" in captured.out


def test_main(monkeypatch):
    monkeypatch.setattr(use_model, "summary_model", lambda: {"r2": 1.0, "mae": 0.0, "mae_pct": 0.0})
    monkeypatch.setattr(use_model, "load_model", lambda: (DummyModel(), "RandomForest_Optimized"))
    monkeypatch.setattr(use_model, "manual_cases", lambda model: None)
    use_model.use_model()


def test_load_data_cleaning(monkeypatch):
    """Test que les lignes invalides sont filtrées"""
    df = pd.DataFrame({
        "distance_km": [0, 20, -5, 100],
        "duration_h": [1, 0, 2, 3],
        "frequency_per_week": [7, 14, 0, 25],
        "train_type": ["TGV", "TER", "TGV", "TGV"],
        "traction": ["électrique", "diesel", "électrique", "électrique"],
        "service_type": ["JOUR", "NUIT", "JOUR", "JOUR"],
        "origin_country": ["FR", "FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "FR", "FR"]
    })
    monkeypatch.setattr(use_model.pd, "read_csv", lambda path: df)
    X, y = use_model.load_data()
    
    assert len(X) == 1
    assert X.iloc[0]["distance_km"] == 100
    assert X.iloc[0]["duration_h"] == 3


def test_load_data_feature_engineering(monkeypatch):
    """Test que le feature engineering fonctionne correctement"""
    df = pd.DataFrame({
        "distance_km": [100, 200],
        "duration_h": [2, 4],
        "frequency_per_week": [10, 20],
        "train_type": ["TGV", "TGV"],
        "traction": ["électrique", "électrique"],
        "service_type": ["JOUR", "NUIT"],
        "origin_country": ["FR", "FR"],
        "destination_country": ["FR", "FR"]
    })
    monkeypatch.setattr(use_model.pd, "read_csv", lambda path: df)
    X, y = use_model.load_data()
    
    # Vérifie les calculs
    assert X.iloc[0]["speed_kmh"] == 50.0  # 100/2
    assert X.iloc[0]["is_night"] == 0
    assert X.iloc[0]["distance_night"] == 0
    assert X.iloc[1]["speed_kmh"] == 50.0  # 200/4
    assert X.iloc[1]["is_night"] == 1
    assert X.iloc[1]["distance_night"] == 200