import os
import sys

import numpy as np
import pandas as pd


sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from train_model_script import extract_data, train_model


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

