import joblib
import numpy as np
import os
import time
import pandas as pd
from middleware.prometheus import (
    PREDICTION_COUNT,
    PREDICTION_LATENCY,
    PREDICTION_VALUE,
)

MODEL_PATH = "/app/models/frequency_model_best.joblib"
ADEME_GCO2E_PKM = 25.0 

NUM_FEATURES_EXTENDED = ["distance_km", "duration_h", "speed_kmh", "is_night", "distance_night"]
CAT_FEATURES = ["service_type", "traction"]
ALL_FEATURES = NUM_FEATURES_EXTENDED + CAT_FEATURES

_model = None
_model_name = None
_last_check = 0
_model_available = False


def is_model_available() -> bool:
    global _last_check, _model_available
    now = time.time()
    if now - _last_check > 300:
        _model_available = os.path.exists(MODEL_PATH)
        _last_check = now
    return _model_available


def load_model():
    global _model, _model_name
    if not os.path.exists(MODEL_PATH):
        print(f"[model_service] Modèle non trouvé à {MODEL_PATH} — réessai automatique toutes les 5 min")
        return
    artifact = joblib.load(MODEL_PATH)
    _model = artifact["model"]
    _model_name = artifact.get("name", "RandomForest_Optimized")
    print(f"[model_service] Modèle '{_model_name}' chargé en RAM depuis {MODEL_PATH}")


def get_model():
    global _model, _model_name
    if _model is None:
        load_model()
    return _model, _model_name


def prepare_features(distance_km, duration_h, service_type, traction):
    speed_kmh = distance_km / duration_h if duration_h > 0 else 0
    is_night = 1 if service_type.upper() == "NUIT" else 0
    distance_night = distance_km if is_night else 0
    
    return {
        "distance_km": distance_km,
        "duration_h": duration_h,
        "speed_kmh": speed_kmh,
        "is_night": is_night,
        "distance_night": distance_night,
        "service_type": service_type.upper(),
        "traction": traction.lower()
    }


def predict_frequency(distance_km, duration_h, service_type="JOUR", traction="électrique"):
    if not is_model_available():
        PREDICTION_COUNT.labels(status="error").inc()
        return {
            "frequency_per_week": None,
            "emission_gco2e_pkm": None,
            "total_emission_kgco2e": None,
            "model": None,
            "warning": "Modèle non disponible"
        }

    start = time.perf_counter()
    try:
        model, name = get_model()
        if model is None:
            PREDICTION_COUNT.labels(status="error").inc()
            return {
                "frequency_per_week": None,
                "emission_gco2e_pkm": None,
                "total_emission_kgco2e": None,
                "model": None,
                "warning": "Modèle non disponible"
            }

        features = prepare_features(distance_km, duration_h, service_type, traction)
        df = pd.DataFrame([features])[ALL_FEATURES]
        
        freq = float(np.clip(model.predict(df)[0], 1, None))
        
        emission_gco2e_pkm = ADEME_GCO2E_PKM
        total_emission_kgco2e = emission_gco2e_pkm * distance_km / 1000

        PREDICTION_COUNT.labels(status="success").inc()
        PREDICTION_LATENCY.observe(time.perf_counter() - start)
        PREDICTION_VALUE.observe(freq)

        return {
            "frequency_per_week": round(freq, 1),
            "emission_gco2e_pkm": emission_gco2e_pkm,
            "total_emission_kgco2e": round(total_emission_kgco2e, 2),
            "model": name,
            "warning": None
        }
    except Exception as exc:
        PREDICTION_COUNT.labels(status="error").inc()
        return {
            "frequency_per_week": None,
            "emission_gco2e_pkm": None,
            "total_emission_kgco2e": None,
            "model": None,
            "warning": f"Erreur modèle : {exc}"
        }


def predict_co2(distance_km, duration_h, nb_stops, train_type, traction):
    return predict_frequency(distance_km, duration_h, "JOUR", traction)