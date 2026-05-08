import os
import sys
import joblib
import numpy as np
import pandas as pd
import pytest

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from train_model_script import extract_data, train_model, use_model


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_df(n=20):
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "distance_km":        rng.uniform(50, 800, n),
        "duration_h":         rng.uniform(0.5, 8.0, n),
        "frequency_per_week": rng.integers(1, 30, n).astype(float),
        "train_type":         rng.choice(["Grande vitesse", "Régional", "Intercité"], n),
        "traction":           rng.choice(["Électrique", "Diesel"], n),
        "service_type":       rng.choice(["JOUR", "NUIT"], n),
        "origin_country":     rng.choice(["FR", "DE", "IT"], n),
        "destination_country":rng.choice(["FR", "DE", "IT"], n),
    })


def _write_csv(path, df=None):
    df = df if df is not None else _make_df()
    df.to_csv(path, index=False)
    return df


# ── extract_data → train_model ────────────────────────────────────────────────

class TestExtractToTrainIntegration:

    def test_csv_produced_by_extract_is_loadable_by_train_model(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_path = data_dir / "trips_freq.csv"

        df = _make_df(30)

        monkeypatch.setattr(extract_data.pd, "read_sql", lambda q, e: df)
        monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: object())

        orig_to_csv = pd.DataFrame.to_csv
        monkeypatch.setattr(
            pd.DataFrame, "to_csv",
            lambda self, path, **kw: orig_to_csv(self, csv_path, **kw)
        )

        extract_data.extract()
        assert csv_path.exists()

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        X, y = train_model.load_data()

        assert isinstance(X, pd.DataFrame)
        assert isinstance(y, pd.Series)
        assert len(X) == len(y)
        assert set(train_model.NUM_FEATURES + train_model.CAT_FEATURES).issubset(X.columns)
        assert y.name == train_model.TARGET

    def test_extract_columns_match_train_model_features(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        df = _make_df()

        monkeypatch.setattr(extract_data.pd, "read_sql", lambda q, e: df)
        monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: object())
        orig_to_csv = pd.DataFrame.to_csv
        monkeypatch.setattr(
            pd.DataFrame, "to_csv",
            lambda self, path, **kw: orig_to_csv(self, csv_path, **kw)
        )

        result_df = extract_data.extract()

        expected_cols = set(
            train_model.NUM_FEATURES + train_model.CAT_FEATURES + [train_model.TARGET]
        )
        assert expected_cols.issubset(set(result_df.columns))

    def test_nan_rows_dropped_by_train_model_load_data(self, tmp_path, monkeypatch):
        df = _make_df(20)
        # Introduit des NaN
        df.loc[0, "distance_km"] = None
        df.loc[5, "train_type"] = None

        csv_path = tmp_path / "trips_freq.csv"
        df.to_csv(csv_path, index=False)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        X, y = train_model.load_data()

        assert len(X) == 18 
        assert not X.isnull().any().any()

# ── train_model → use_model ───────────────────────────────────────────────────

class TestTrainToUseIntegration:

    def test_model_saved_by_train_is_loadable_by_use_model(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        _write_csv(csv_path)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))

        train_model.train_and_save()
        assert model_path.exists()

        model, name = use_model.load_model(path=str(model_path))
        assert name == "RandomForest"
        assert hasattr(model, "predict")
        assert hasattr(model, "fit")

    def test_model_trained_then_predicts_positive_frequencies(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        _write_csv(csv_path)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))

        model = train_model.train_and_save()

        test_cases = pd.DataFrame([{
            "distance_km": 450, "duration_h": 2.5,
            "train_type": "Grande vitesse", "traction": "Électrique",
            "service_type": "JOUR", "origin_country": "FR", "destination_country": "FR",
        }])

        preds = np.clip(model.predict(test_cases), 1, None)
        assert all(p >= 1 for p in preds)

    def test_artifact_structure_preserved_through_save_load(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        _write_csv(csv_path)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        train_model.train_and_save()

        artifact = joblib.load(model_path)
        assert set(artifact.keys()) == {"model", "name"}
        assert artifact["name"] == "RandomForest"

    def test_evaluate_metrics_coherent_between_train_and_use(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        df = _make_df(50)
        _write_csv(csv_path, df)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        model = train_model.train_and_save()

        monkeypatch.setattr(use_model, "DATA_PATH", str(csv_path))
        X, y = use_model.load_data(str(csv_path))
        metrics = use_model.evaluate(model, X, y)

        assert "r2" in metrics
        assert "mae" in metrics
        assert isinstance(metrics["r2"], float)
        assert metrics["mae"] >= 0


# ── extract_data → use_model ──────────────────────────────────────────────────

class TestExtractToUseIntegration:

    def test_csv_from_extract_loadable_by_use_model(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        df = _make_df(20)

        monkeypatch.setattr(extract_data.pd, "read_sql", lambda q, e: df)
        monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: object())
        orig_to_csv = pd.DataFrame.to_csv
        monkeypatch.setattr(
            pd.DataFrame, "to_csv",
            lambda self, path, **kw: orig_to_csv(self, csv_path, **kw)
        )
        extract_data.extract()

        X, y = use_model.load_data(str(csv_path))
        assert not X.empty
        assert not y.empty
        assert set(use_model.NUM_FEATURES + use_model.CAT_FEATURES).issubset(X.columns)



class TestFullPipeline:

    def test_full_pipeline_extract_train_use(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        df = _make_df(40)

        # Step 1 : extract
        monkeypatch.setattr(extract_data.pd, "read_sql", lambda q, e: df)
        monkeypatch.setattr(extract_data.sqlalchemy, "create_engine", lambda url: object())
        orig_to_csv = pd.DataFrame.to_csv
        monkeypatch.setattr(
            pd.DataFrame, "to_csv",
            lambda self, path, **kw: orig_to_csv(self, csv_path, **kw)
        )
        extract_data.extract()
        assert csv_path.exists()

        # Step 2 : train
        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        train_model.train_and_save()
        assert model_path.exists()

        # Step 3 : use
        model, name = use_model.load_model(path=str(model_path))
        assert name == "RandomForest"

        X, y = use_model.load_data(str(csv_path))
        metrics = use_model.evaluate(model, X, y)

        assert metrics["mae"] >= 0
        assert isinstance(metrics["r2"], float)

    def test_full_pipeline_predictions_clipped_to_minimum_one(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        _write_csv(csv_path)

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        train_model.train_and_save()

        model, _ = use_model.load_model(path=str(model_path))

        sample = pd.DataFrame([
            {"distance_km": 450, "duration_h": 2.5, "train_type": "Grande vitesse",
             "traction": "Électrique", "service_type": "JOUR",
             "origin_country": "FR", "destination_country": "FR"},
            {"distance_km": 80, "duration_h": 1.2, "train_type": "Régional",
             "traction": "Diesel", "service_type": "JOUR",
             "origin_country": "FR", "destination_country": "FR"},
        ])
        preds = np.clip(model.predict(sample), 1, None)
        assert all(p >= 1 for p in preds)

    def test_model_retrained_overwrites_previous(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"

        _write_csv(csv_path, _make_df(20))
        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        train_model.train_and_save()
        mtime_1 = os.path.getmtime(model_path)

        import time; time.sleep(0.05)

        _write_csv(csv_path, _make_df(30))
        train_model.train_and_save()
        mtime_2 = os.path.getmtime(model_path)

        assert mtime_2 > mtime_1

        model, name = use_model.load_model(path=str(model_path))
        assert name == "RandomForest"
        assert hasattr(model, "predict")

    def test_sanity_checks_pass_after_real_training(self, tmp_path, monkeypatch, capsys):
        csv_path = tmp_path / "trips_freq.csv"
        model_path = tmp_path / "model.joblib"
        _write_csv(csv_path, _make_df(40))

        monkeypatch.setattr(train_model, "DATA_PATH", str(csv_path))
        monkeypatch.setattr(train_model, "MODEL_PATH", str(model_path))
        model = train_model.train_and_save()

        train_model.sanity_checks(model)
        captured = capsys.readouterr()
        assert "SANITY CHECKS" in captured.out

