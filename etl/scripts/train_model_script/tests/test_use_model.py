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
    artifact = {"model": DummyModel(), "name": "Dummy"}
    model_path = tmp_path / "model.joblib"
    joblib.dump(artifact, model_path)
    model, name = use_model.load_model(path=str(model_path))
    assert name == "Dummy"
    assert hasattr(model, "predict")

def test_load_data(monkeypatch):
    df = pd.DataFrame({
        "distance_km": [10, 20],
        "duration_h": [1, 2],
        "frequency_per_week": [7, 14],
        "train_type": ["TGV", "TER"],
        "traction": ["Électrique", "Diesel"],
        "service_type": ["JOUR", "NUIT"],
        "origin_country": ["FR", "FR"],
        "destination_country": ["FR", "FR"]
    })
    monkeypatch.setattr(use_model.pd, "read_csv", lambda path: df)
    X, y = use_model.load_data()
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert not X.empty
    assert not y.empty

def test_evaluate():
    X = pd.DataFrame({"a": [1, 2, 3]})
    y = pd.Series([1, 1, 1])
    metrics = use_model.evaluate(DummyModel(), X, y)
    assert "r2" in metrics
    assert "mae" in metrics
    assert "mae_pct" in metrics
    assert "y_test" in metrics
    assert "y_pred" in metrics

def test_summary_model(monkeypatch, capsys):
    monkeypatch.setattr(use_model, "load_model", lambda: (DummyModel(), "Dummy"))
    df = pd.DataFrame({
        "distance_km": [10, 20],
        "duration_h": [1, 2],
        "frequency_per_week": [7, 14],
        "train_type": ["TGV", "TER"],
        "traction": ["Électrique", "Diesel"],
        "service_type": ["JOUR", "NUIT"],
        "origin_country": ["FR", "FR"],
        "destination_country": ["FR", "FR"]
    })
    monkeypatch.setattr(use_model, "load_data", lambda: (df[use_model.NUM_FEATURES + use_model.CAT_FEATURES], df[use_model.TARGET]))
    metrics = use_model.summary_model()
    captured = capsys.readouterr()
    assert "Modèle" in captured.out
    assert isinstance(metrics, dict)
    assert "r2" in metrics

def test_manual_cases(capsys):
    use_model.manual_cases(DummyModel())
    captured = capsys.readouterr()
    assert "CAS MANUELS" in captured.out

def test_main(monkeypatch):
    monkeypatch.setattr(use_model, "summary_model", lambda: {"r2": 1.0, "mae": 0.0, "mae_pct": 0.0})
    monkeypatch.setattr(use_model, "load_model", lambda: (DummyModel(), "Dummy"))
    monkeypatch.setattr(use_model, "manual_cases", lambda model: None)
    use_model.use_model()