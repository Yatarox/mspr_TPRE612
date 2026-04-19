from fastapi import APIRouter, Query
from typing import Optional
from services import model_service

router = APIRouter()

@router.get("/predict")
def predict(
    distance_km: float = Query(..., description="Distance du trajet en km"),
    duration_h: float = Query(..., description="Durée du trajet en heures"),
    nb_stops: int = Query(0, description="Nombre d'arrêts"),
    train_type: str = Query(..., description="Type de train"),
    traction: str = Query(..., description="Type de traction"),
):
    result = model_service.predict_co2(
        distance_km=distance_km,
        duration_h=duration_h,
        nb_stops=nb_stops,
        train_type=train_type,
        traction=traction
    )
    return result