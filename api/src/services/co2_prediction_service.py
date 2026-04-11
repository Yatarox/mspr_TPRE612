
import logging

import joblib
import numpy as np
import pandas as pd
from huggingface_hub import hf_hub_download

logger = logging.getLogger(__name__)

# ── Constantes du modèle ───────────────────────────────────────────────────────

HF_MODEL_REPO = "Yatarox/train-co2-regressor"
HF_MODEL_FILE = "model.joblib"

FALLBACK_COEFFICIENTS = {
    "Électrique": 6.0,
    "Diesel":     41.0,
    "Hybride":    22.0,
    "Autre":      30.0,
}

TRAIN_TYPE_FACTOR = {
    "TGV":        0.85,
    "TER":        1.10,
    "Intercités": 1.00,
    "Fret":       1.50,
    "Autre":      1.00,
}

_pipeline = None
_use_fallback: bool = False


# ── Chargement du modèle ───────────────────────────────────────────────────────

def load_model():
    """
    Tente de charger le pipeline scikit-learn depuis HuggingFace Hub.
    En cas d'échec, bascule sur le fallback ADEME/IEA.
    """
    global _pipeline, _use_fallback

    try:
        logger.info("Téléchargement du modèle CO2 depuis HuggingFace : %s", HF_MODEL_REPO)
        model_path = hf_hub_download(repo_id=HF_MODEL_REPO, filename=HF_MODEL_FILE)
        _pipeline = joblib.load(model_path)
        _use_fallback = False
        logger.info("Modèle CO2 chargé avec succès.")

    except Exception as exc:
        logger.warning(
            "Modèle HuggingFace indisponible (%s). "
            "Utilisation du fallback ADEME/IEA.",
            exc,
        )
        _pipeline = None
        _use_fallback = True


def get_model():
    """Retourne le pipeline (chargé une seule fois)."""
    global _pipeline, _use_fallback
    if _pipeline is None and not _use_fallback:
        load_model()
    return _pipeline


# ── Prédiction ─────────────────────────────────────────────────────────────────

def _fallback_predict(
    distance_km: float,
    traction: str,
    train_type: str,
) -> dict:
    """
    Estimation ADEME/IEA lorsque le modèle ML n'est pas disponible.
    """
    base = FALLBACK_COEFFICIENTS.get(traction, FALLBACK_COEFFICIENTS["Autre"])
    factor = TRAIN_TYPE_FACTOR.get(train_type, 1.0)
    emission_per_km = round(base * factor, 2)
    total = round(emission_per_km * distance_km / 1000, 4)

    return {
        "emission_gco2e_pkm": emission_per_km,
        "total_emission_kgco2e": total,
        "model": "fallback-ademe-iea",
        "warning": "Modèle ML indisponible ; estimation basée sur les coefficients ADEME/IEA.",
    }


def predict_co2(
    distance_km: float,
    duration_h: float,
    nb_stops: int,
    train_type: str,
    traction: str,
) -> dict:
    """
    Prédit les émissions CO2 d'un trajet ferroviaire.
    """
    pipeline = get_model()

    if pipeline is None:
        return _fallback_predict(distance_km, traction, train_type)

    try:
        X = pd.DataFrame([{
            "distance_km": distance_km,
            "duration_h": duration_h,
            "nb_stops": nb_stops,
            "train_type": train_type,
            "traction": traction,
        }])

        emission_per_km = float(pipeline.predict(X)[0])
        emission_per_km = max(0.0, round(emission_per_km, 2))
        total = round(emission_per_km * distance_km / 1000, 4)

        return {
            "emission_gco2e_pkm": emission_per_km,
            "total_emission_kgco2e": total,
            "model": "hf-pipeline",
        }

    except Exception as exc:
        logger.error("Erreur lors de la prédiction ML : %s", exc)
        fallback = _fallback_predict(distance_km, traction, train_type)
        fallback["warning"] = f"Erreur pipeline ML ({exc}) ; fallback ADEME utilisé."
        return fallback