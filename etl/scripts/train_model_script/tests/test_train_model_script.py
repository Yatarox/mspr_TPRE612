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
        "traction": ["électrique", "diesel"],
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
    assert "speed_kmh" in X.columns
    assert "is_night" in X.columns
    assert "distance_night" in X.columns
    assert "service_type" in X.columns
    assert "traction" in X.columns


def test_build_model():
    model = train_model.build_model()
    assert hasattr(model, "fit")
    assert hasattr(model, "predict")
    
    # Vérifie que c'est un pipeline avec RandomForest
    from sklearn.pipeline import Pipeline
    from sklearn.ensemble import RandomForestRegressor
    assert isinstance(model, Pipeline)
    assert isinstance(model.named_steps['randomforestregressor'], RandomForestRegressor)


def test_train_and_save(tmp_path, monkeypatch):
    monkeypatch.setattr(train_model, "MODEL_PATH", str(tmp_path / "model.joblib"))
    
    df = pd.DataFrame({
        "distance_km": [10, 20, 30, 40, 50, 60, 70, 80],
        "duration_h": [1, 2, 3, 4, 5, 6, 7, 8],
        "frequency_per_week": [7, 14, 21, 28, 35, 42, 49, 56],
        "train_type": ["TGV", "TER", "TGV", "TER", "TGV", "TER", "TGV", "TER"],
        "traction": ["électrique", "diesel", "électrique", "diesel", "électrique", "diesel", "électrique", "diesel"],
        "service_type": ["JOUR", "NUIT", "JOUR", "NUIT", "JOUR", "NUIT", "JOUR", "NUIT"],
        "origin_country": ["FR", "FR", "FR", "FR", "FR", "FR", "FR", "FR"],
        "destination_country": ["FR", "FR", "FR", "FR", "FR", "FR", "FR", "FR"]
    })
    
    def mock_load_data():
        df_copy = df.copy()
        df_copy['speed_kmh'] = df_copy['distance_km'] / df_copy['duration_h']
        df_copy['is_night'] = (df_copy['service_type'] == 'NUIT').astype(int)
        df_copy['distance_night'] = df_copy['distance_km'] * df_copy['is_night']
        X = df_copy[train_model.NUM_FEATURES_EXTENDED + train_model.CAT_FEATURES]
        y = df_copy[train_model.TARGET]
        return X, y
    
    monkeypatch.setattr(train_model, "load_data", mock_load_data)
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
    
    X_test = pd.DataFrame({
        "distance_km": [10, 20, 30],
        "duration_h": [1, 2, 3],
        "speed_kmh": [10, 10, 10],
        "is_night": [0, 0, 0],
        "distance_night": [0, 0, 0],
        "service_type": ["JOUR", "JOUR", "JOUR"],
        "traction": ["électrique", "électrique", "électrique"]
    })
    y_test = pd.Series([1, 1, 1])
    
    train_model.evaluate(DummyModel(), X_test, y_test)
    captured = capsys.readouterr()
    assert "R²" in captured.out
    assert "MAE" in captured.out


def test_manual_cases(capsys):
    class DummyModel:
        def predict(self, X):
            return np.array([15, 8, 20, 5, 30])
    
    train_model.manual_cases(DummyModel())
    captured = capsys.readouterr()
    assert "CAS MANUELS" in captured.out
    assert "TGV Paris→Lyon" in captured.out
    assert "TER court" in captured.out
    assert "International FR→DE" in captured.out
    assert "Train de nuit" in captured.out
    assert "RER banlieue" in captured.out


def test_sanity_checks(capsys):
    class DummyModel:
        def predict(self, X):
            if X['is_night'].iloc[0] == 0:
                return np.array([20])
            else:
                return np.array([10])
    
    train_model.sanity_checks(DummyModel())
    captured = capsys.readouterr()
    assert "SANITY CHECKS" in captured.out


def test_sanity_checks_fails(capsys):
    class DummyModel:
        def predict(self, X):
            if X['is_night'].iloc[0] == 0:
                return np.array([5])
            else:
                return np.array([20])
    
    train_model.sanity_checks(DummyModel())
    captured = capsys.readouterr()
    assert "incohérent" in captured.out


def test_main(monkeypatch):
    monkeypatch.setattr(train_model, "train_and_save", lambda: type("M", (), {"predict": lambda self, X: np.ones(len(X))})())
    monkeypatch.setattr(train_model, "manual_cases", lambda model: None)
    monkeypatch.setattr(train_model, "sanity_checks", lambda model: None)
    train_model.train()


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
    monkeypatch.setattr(train_model.pd, "read_csv", lambda path: df)
    X, y = train_model.load_data()
    
    assert len(X) == 1
    assert X.iloc[0]["distance_km"] == 100
    assert X.iloc[0]["duration_h"] == 3


def test_feature_engineering_calculations(monkeypatch):
    """Test que le feature engineering calcule correctement"""
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
    monkeypatch.setattr(train_model.pd, "read_csv", lambda path: df)
    X, y = train_model.load_data()
    
    assert X.iloc[0]["speed_kmh"] == 50.0
    assert X.iloc[0]["is_night"] == 0
    assert X.iloc[0]["distance_night"] == 0
    assert X.iloc[1]["speed_kmh"] == 50.0
    assert X.iloc[1]["is_night"] == 1
    assert X.iloc[1]["distance_night"] == 200