from fastapi import APIRouter, Query
from typing import Optional
from services import dashboard_service

router = APIRouter()


@router.get("/stats/overview")
async def get_overview():
    """KPIs généraux du data warehouse"""
    return await dashboard_service.get_overview()


@router.get("/stats/by-country")
async def get_stats_by_country():
    """Statistiques par pays d'origine"""
    return await dashboard_service.get_stats_by_country()


@router.get("/stats/by-train-type")
async def get_stats_by_train_type():
    """Statistiques par type de train"""
    return await dashboard_service.get_stats_by_train_type()


@router.get("/stats/by-traction")
async def get_stats_by_traction():
    """Statistiques par type de traction"""
    return await dashboard_service.get_stats_by_traction()


@router.get("/stats/by-agency")
async def get_stats_by_agency(
    limit: int = Query(default=10, ge=1, le=100)
):
    """Top agences par nombre de trajets"""
    return await dashboard_service.get_stats_by_agency(limit)


@router.get("/emissions/by-route")
async def get_emissions_by_route(
    limit: int = Query(default=20, ge=1, le=100)
):
    """Routes les plus émettrices"""
    return await dashboard_service.get_emissions_by_route(limit)


@router.get("/trips/search")
async def search_trips(
    origin: Optional[str] = Query(None, description="Ville d'origine"),
    destination: Optional[str] = Query(None, description="Ville de destination"),
    train_type: Optional[str] = Query(None, description="Type de train"),
    min_distance: Optional[float] = Query(None, ge=0),
    max_distance: Optional[float] = Query(None, ge=0),
    limit: int = Query(default=50, ge=1, le=1000),
):
    return await dashboard_service.search_trips(
        origin=origin,
        destination=destination,
        train_type=train_type,
        min_distance=min_distance,
        max_distance=max_distance,
        limit=limit,
    )

@router.get("/stats/by-service-type")
async def get_stats_by_service_type():
    return await dashboard_service.get_stats_by_service_type()