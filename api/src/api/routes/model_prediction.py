from fastapi import APIRouter, Query
from services import model_service

router = APIRouter()

@router.get("/predict")
def predict(
    distance_km: float = Query(..., description="Distance du trajet en km", ge=0),
    duration_h: float = Query(..., description="Durée du trajet en heures", gt=0),
    traction: str = Query(..., description="Type de traction (électrique, diesel, mixte)"),
    service_type: str = Query(..., description="Type de service (JOUR ou NUIT)"),
    nb_stops: int = Query(0, description="Nombre d'arrêts (ignoré pour ce modèle)"),
    train_type: str = Query("Rail", description="Type de train (ignoré pour ce modèle)"),
):
    result = model_service.predict_frequency(
        distance_km=distance_km,
        duration_h=duration_h,
        service_type=service_type,
        traction=traction
    )
    return result