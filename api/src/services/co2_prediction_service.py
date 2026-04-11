"""
Service de prédiction d'émissions CO2 pour les trains.

Modèle : scikit-learn pipeline entraîné sur des données ferroviaires,
hébergé sur Hugging Face Hub (joblib).

Features attendues :
    - distance_km       : distance du trajet (km)
    - duration_h        : durée du trajet (h)
    - nb_stops          : nombre d'arrêts intermédiaires
    - train_type        : type de train (TGV, TER, Intercités, Fret, Autre)
    - traction          : mode de traction (Électrique, Diesel, Hybride, Autre)

Output :
    - emission_gco2e_pkm : émissions estimées en gCO2e par passager-km
    - total_emission_kgco2e : émissions totales estimées pour le trajet
"""

import logging


logger = logging.getLogger(__name__)

# ── Constantes du modèle ───────────────────────────────────────────────────────

# Repo HuggingFace contenant le pipeline joblib
HF_MODEL_REPO = "Yatarox/train-co2-regressor"
HF_MODEL_FILE = "model.joblib"

# Fallback : coefficients issus de la littérature IEA / ADEME si le modèle
# HF n'est pas encore disponible (on log un warning).
FALLBACK_COEFFICIENTS = {
    # gCO2e / passager-km selon le type de traction
    "Électrique": 6.0,
    "Diesel":     41.0,
    "Hybride":    22.0,
    "Autre":      30.0,
}

TRAIN_TYPE_FACTOR = {
    # Facteur multiplicateur selon le type de train (distance, vitesse, masse)
    "TGV":        0.85,
    "TER":        1.10,
    "Intercités": 1.00,
    "Fret":       1.50,   # fret ≠ passager, mais on garde pour cohérence
    "Autre":      1.00,
}

_pipeline = None          # cache du modèle chargé
_use_fallback: bool = False


# ── Chargement du modèle ───────────────────────────────────────────────────────

def load_model():
    """
    Tente de charger le pipeline scikit-learn depuis HuggingFace Hub.
    En cas d'échec (repo absent, pas de réseau…), bascule sur le fallback ADEME.
    """
    global _pipeline, _use_fallback

    try:
        from huggingface_hub import hf_hub_download
        import joblib

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
    Formule : emission_per_km = base_traction × factor_train_type
    """
    base = FALLBACK_COEFFICIENTS.get(traction, FALLBACK_COEFFICIENTS["Autre"])
    factor = TRAIN_TYPE_FACTOR.get(train_type, 1.0)
    emission_per_km = round(base * factor, 2)
    total = round(emission_per_km * distance_km / 1000, 4)  # kg

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

    Returns
    -------
    dict avec les clés :
        - emission_gco2e_pkm
        - total_emission_kgco2e
        - model  ("hf-pipeline" | "fallback-ademe-iea")
        - warning (optionnel)
    """
    pipeline = get_model()

    if pipeline is None:
        return _fallback_predict(distance_km, traction, train_type)

    try:
        # Le pipeline HF attend un tableau numpy avec les features dans l'ordre :
        # [distance_km, duration_h, nb_stops, train_type_encoded, traction_encoded]
        # Le ColumnTransformer intégré gère l'encodage des catégorielles.
        import pandas as pd
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
