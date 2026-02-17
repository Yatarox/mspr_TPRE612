from pydantic import BaseModel
from typing import List, Optional

class DashboardMetric(BaseModel):
    id: int
    name: str
    value: float
    timestamp: str

class DashboardCreate(BaseModel):
    name: str
    value: float

class DashboardUpdate(BaseModel):
    name: Optional[str] = None
    value: Optional[float] = None

class DashboardResponse(BaseModel):
    metrics: List[DashboardMetric]