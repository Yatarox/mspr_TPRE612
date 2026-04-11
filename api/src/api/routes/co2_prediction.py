from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal
import time

from services.co2_prediction_service import predict_co2
from middleware.prometheus import PREDICTION_COUNT, PREDICTION_LATENCY, PREDICTION_VALUE

router = APIRouter()


# ── Schémas ────────────────────────────────────────────────────────────────────

class CO2PredictionRequest(BaseModel):
    distance_km: float = Field(..., gt=0, le=20_000, description="Distance du trajet en km")
    duration_h: float = Field(..., gt=0, le=100, description="Durée du trajet en heures")
    nb_stops: int = Field(default=0, ge=0, le=500, description="Nombre d'arrêts intermédiaires")
    train_type: Literal["TGV", "TER", "Intercités", "Fret", "Autre"] = Field(
        default="Autre", description="Type de train"
    )
    traction: Literal["Électrique", "Diesel", "Hybride", "Autre"] = Field(
        default="Électrique", description="Mode de traction"
    )

    model_config = {"json_schema_extra": {
        "example": {
            "distance_km": 450.0,
            "duration_h": 2.5,
            "nb_stops": 3,
            "train_type": "TGV",
            "traction": "Électrique",
        }
    }}


class CO2PredictionResponse(BaseModel):
    # Inputs renvoyés pour traçabilité
    distance_km: float
    duration_h: float
    nb_stops: int
    train_type: str
    traction: str

    # Outputs du modèle
    emission_gco2e_pkm: float = Field(..., description="Émissions en gCO2e par passager-km")
    total_emission_kgco2e: float = Field(..., description="Émissions totales estimées en kgCO2e")
    model: str = Field(..., description="Modèle utilisé pour la prédiction")
    warning: Optional[str] = Field(None, description="Avertissement si fallback utilisé")


# ── Route ──────────────────────────────────────────────────────────────────────

@router.post(
    "/predict/co2",
    response_model=CO2PredictionResponse,
    summary="Prédiction d'émissions CO2 d'un trajet ferroviaire",
    description=(
        "Prédit les émissions CO2 (gCO2e/passager-km et total kgCO2e) "
        "à partir des caractéristiques d'un trajet ferroviaire. "
        "Utilise un modèle de régression linéaire entraîné sur des données ferroviaires européennes. "
        "Bascule automatiquement sur les coefficients ADEME/IEA si le modèle ML est indisponible."
    ),
    tags=["predict"],
)
async def predict_co2_route(payload: CO2PredictionRequest):
    start = time.perf_counter()

    try:
        result = predict_co2(
            distance_km=payload.distance_km,
            duration_h=payload.duration_h,
            nb_stops=payload.nb_stops,
            train_type=payload.train_type,
            traction=payload.traction,
        )
    except Exception as exc:
        PREDICTION_COUNT.labels(status="error").inc()
        raise HTTPException(status_code=500, detail=f"Erreur de prédiction : {exc}")

    duration = time.perf_counter() - start
    PREDICTION_LATENCY.observe(duration)
    PREDICTION_COUNT.labels(status="success").inc()
    PREDICTION_VALUE.observe(result["emission_gco2e_pkm"])

    return CO2PredictionResponse(
        distance_km=payload.distance_km,
        duration_h=payload.duration_h,
        nb_stops=payload.nb_stops,
        train_type=payload.train_type,
        traction=payload.traction,
        **result,
    )
