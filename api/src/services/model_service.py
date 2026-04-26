import joblib
import numpy as np
import os
import time

from middleware.prometheus import (
    PREDICTION_COUNT,
    PREDICTION_LATENCY,
    PREDICTION_VALUE,
)

MODEL_PATH = "/app/models/frequency_model.joblib"
ADEME_GCO2E_PKM = 1.7

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
    """Charge le modèle en RAM. Appelé au démarrage via lifespan.
    Si le fichier est absent, le service fonctionne sans modèle et
    réessaiera automatiquement toutes les 5 min via is_model_available()."""
    global _model, _model_name
    if not os.path.exists(MODEL_PATH):
        print(f"[model_service] Modèle non trouvé à {MODEL_PATH} — réessai automatique toutes les 5 min")
        return
    artifact = joblib.load(MODEL_PATH)
    _model = artifact["model"]
    _model_name = artifact.get("name", "RandomForest")
    print(f"[model_service] Modèle '{_model_name}' chargé en RAM depuis {MODEL_PATH}")


def get_model():
    global _model, _model_name
    if _model is None:
        load_model()
    return _model, _model_name


def predict_co2(distance_km, duration_h, nb_stops, train_type, traction):
    if not is_model_available():
        PREDICTION_COUNT.labels(status="error").inc()
        return {
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
                "emission_gco2e_pkm": None,
                "total_emission_kgco2e": None,
                "model": None,
                "warning": "Modèle non disponible"
            }

        import pandas as pd
        features = {
            "distance_km": distance_km,
            "duration_h": duration_h,
            "train_type": train_type,
            "traction": traction,
            "service_type": "JOUR",
            "origin_country": "FR",
            "destination_country": "FR",
        }
        df = pd.DataFrame([features])
        freq = float(np.clip(model.predict(df)[0], 1, None))
        emission_gco2e_pkm = ADEME_GCO2E_PKM
        total_emission_kgco2e = emission_gco2e_pkm * distance_km / 1000

        PREDICTION_COUNT.labels(status="success").inc()
        PREDICTION_LATENCY.observe(time.perf_counter() - start)
        PREDICTION_VALUE.observe(emission_gco2e_pkm)

        return {
            "frequency_per_week": freq,
            "emission_gco2e_pkm": emission_gco2e_pkm,
            "total_emission_kgco2e": total_emission_kgco2e,
            "model": name,
            "warning": None
        }
    except Exception as exc:
        PREDICTION_COUNT.labels(status="error").inc()
        return {
            "emission_gco2e_pkm": None,
            "total_emission_kgco2e": None,
            "model": None,
            "warning": f"Erreur modèle : {exc}"
        }