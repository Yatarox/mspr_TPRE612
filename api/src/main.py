from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api.routes.dashboard import router as dashboard_router
from models.database import init_db_pool, close_db_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    yield
    await close_db_pool()

app = FastAPI(
    title="Rail Data Warehouse API",
    description="API pour le dashboard de données ferroviaires GTFS",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router, prefix="/api", tags=["dashboard"])


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
