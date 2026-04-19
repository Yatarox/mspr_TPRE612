import pytest
import numpy as np
import pandas as pd
import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
from train_model_script import train_model

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
    monkeypatch.setattr(train_model.pd, "read_csv", lambda path: df)
    X, y = train_model.load_data()
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert not X.empty
    assert not y.empty

def test_build_model():
    model = train_model.build_model()
    assert hasattr(model, "fit")
    assert hasattr(model, "predict")

def test_train_and_save(tmp_path, monkeypatch):
    # Patch MODEL_PATH to a temp file
    monkeypatch.setattr(train_model, "MODEL_PATH", str(tmp_path / "model.joblib"))
    # Patch load_data to return dummy data
    df = pd.DataFrame({
        "distance_km": [10, 20, 30, 40],
        "duration_h": [1, 2, 3, 4],
        "frequency_per_week": [7, 14, 21, 28],
        "train_type": ["TGV", "TER", "TGV", "TER"],
        "traction": ["Électrique", "Diesel", "Électrique", "Diesel"],
        "service_type": ["JOUR", "NUIT", "JOUR", "NUIT"],
        "origin_country": ["FR", "FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "FR", "FR"]
    })
    monkeypatch.setattr(train_model, "load_data", lambda: (df[train_model.NUM_FEATURES + train_model.CAT_FEATURES], df[train_model.TARGET]))
    model = train_model.train_and_save()
    assert (tmp_path / "model.joblib").exists()
    X, _ = train_model.load_data()
    preds = model.predict(X.head(2))
    assert isinstance(preds, np.ndarray)
    assert preds.shape[0] == 2

def test_evaluate(monkeypatch, capsys):

    class DummyModel:
        def predict(self, X):
            return np.ones(len(X))
    X_test = pd.DataFrame({"a": [1, 2, 3]})
    y_test = pd.Series([1, 1, 1])
    train_model.evaluate(DummyModel(), X_test, y_test)
    captured = capsys.readouterr()
    assert "R²" in captured.out

def test_manual_cases(capsys):

    class DummyModel:
        def predict(self, X):
            return np.ones(len(X))
    train_model.manual_cases(DummyModel())
    captured = capsys.readouterr()
    assert "CAS MANUELS" in captured.out

def test_sanity_checks(capsys):

    class DummyModel:
        def predict(self, X):
            return np.ones(len(X))
    train_model.sanity_checks(DummyModel())
    captured = capsys.readouterr()
    assert "SANITY CHECKS" in captured.out

def test_main(monkeypatch):
    # Patch all called functions to avoid side effects
    monkeypatch.setattr(train_model, "train_and_save", lambda: type("M", (), {"predict": lambda self, X: np.ones(len(X))})())
    monkeypatch.setattr(train_model, "manual_cases", lambda model: None)
    monkeypatch.setattr(train_model, "sanity_checks", lambda model: None)
    train_model.train()