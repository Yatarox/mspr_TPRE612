from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from api.routes.dashboard import router as dashboard_router
from api.routes.model_prediction import router as co2_router
from models.database import init_db_pool, close_db_pool
from middleware.prometheus import PrometheusMiddleware
from services.model_service import load_model


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    load_model()  # chargement du modèle en RAM au démarrage
    yield
    await close_db_pool()


app = FastAPI(
    title="Rail Data Warehouse API",
    description="API pour le dashboard de données ferroviaires GTFS",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(PrometheusMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router, prefix="/api", tags=["dashboard"])
app.include_router(co2_router, prefix="/api", tags=["predict"])


@app.get("/")
def read_root():
    return {
        "name": "Rail Data Warehouse API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    from services.dashboard_service import get_health
    return await get_health()


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )