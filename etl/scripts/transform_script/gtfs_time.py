import gc
from typing import Optional

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def parse_gtfs_time_to_sec(t: str) -> Optional[int]:
    if not isinstance(t, str) or not t.strip():
        return None
    parts = t.strip().split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def _parse_time_vectorized(series: pd.Series) -> pd.Series:
    """Convertit une Series HH:MM:SS en secondes, vectorisé sans apply."""
    s = series.astype(str).str.strip()
    parts = s.str.split(":", expand=True).reindex(columns=[0, 1, 2])
    h = pd.to_numeric(parts[0], errors="coerce").fillna(0)
    m = pd.to_numeric(parts[1], errors="coerce").fillna(0)
    sec = pd.to_numeric(parts[2], errors="coerce").fillna(0)
    result = h * 3600 + m * 60 + sec
    # Les valeurs qui étaient NaN/vides repassent à NaN
    result[series.isna() | (series.astype(str).str.strip() == "") | (series.astype(str).str.strip() == "nan")] = np.nan
    return result


def classifier_train(departure_time: str) -> str:
    try:
        h = int(departure_time.split(":")[0])
        return "NUIT" if h >= 22 or h < 6 else "JOUR"
    except Exception:
        return "INCONNU"


def compute_durations(stop_times: pd.DataFrame, chunk_size: int = 10000) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)

    required = ["trip_id", "arrival_time", "departure_time", "stop_sequence"]
    missing = [col for col in required if col not in stop_times.columns]
    if missing:
        logger.warning(f"⚠️ compute_durations: colonnes manquantes {missing}")
        logger.warning(f"   Colonnes disponibles: {list(stop_times.columns)}")
        return pd.Series(dtype=float)

    st = stop_times[required].copy()

    # Vectorisé : pas de apply() pour le parsing
    st["arr_sec"] = _parse_time_vectorized(st["arrival_time"])
    st["dep_sec"] = _parse_time_vectorized(st["departure_time"])
    st = st.drop(columns=["arrival_time", "departure_time"])
    st = st.sort_values(["trip_id", "stop_sequence"])

    trip_ids = st["trip_id"].unique()
    results = []

    for i in range(0, len(trip_ids), chunk_size):
        chunk_trip_ids = trip_ids[i:i + chunk_size]
        chunk = st[st["trip_id"].isin(chunk_trip_ids)].copy()

        g = chunk.groupby("trip_id")
        first_dep = g["dep_sec"].first()
        last_arr = g["arr_sec"].last()
        dur_sec = last_arr - first_dep

        # Remplacement vectorisé de minmax_span — plus de apply()
        # min/max sur arr_sec et dep_sec combinés par trip
        arr_min = g["arr_sec"].min()
        arr_max = g["arr_sec"].max()
        dep_min = g["dep_sec"].min()
        dep_max = g["dep_sec"].max()
        overall_min = pd.concat([arr_min, dep_min], axis=1).min(axis=1)
        overall_max = pd.concat([arr_max, dep_max], axis=1).max(axis=1)
        alt = (overall_max - overall_min).clip(lower=0).fillna(0)

        dur_sec = dur_sec.where(dur_sec >= 0, alt).fillna(0)
        result = (dur_sec / 60.0).round(2)
        results.append(result)

        del chunk, g, first_dep, last_arr, dur_sec
        del arr_min, arr_max, dep_min, dep_max, overall_min, overall_max, alt, result
        gc.collect()

    del st
    gc.collect()

    if results:
        return pd.concat(results)
    else:
        return pd.Series(dtype=float)