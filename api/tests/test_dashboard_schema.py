import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from schemas.dashboard import DashboardMetric, DashboardCreate, DashboardUpdate, DashboardResponse

def test_dashboard_metric_valid():
    metric = DashboardMetric(id=1, name="CO2", value=12.5, timestamp="2024-02-23T12:00:00Z")
    assert metric.id == 1
    assert metric.name == "CO2"
    assert metric.value == 12.5

def test_dashboard_create_valid():
    create = DashboardCreate(name="CO2", value=10.0)
    assert create.name == "CO2"
    assert create.value == 10.0

def test_dashboard_update_partial():
    update = DashboardUpdate(name="CO2")
    assert update.name == "CO2"
    assert update.value is None

def test_dashboard_response_valid():
    metric = DashboardMetric(id=1, name="CO2", value=12.5, timestamp="2024-02-23T12:00:00Z")
    resp = DashboardResponse(metrics=[metric])
    assert len(resp.metrics) == 1
    assert resp.metrics[0].name == "CO2"