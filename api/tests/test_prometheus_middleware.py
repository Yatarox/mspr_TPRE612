import sys
import os
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse, Response

from middleware.prometheus import PrometheusMiddleware, REQUEST_COUNT, REQUEST_LATENCY


# ── App Starlette minimale pour tester le middleware seul ─────────────────────

async def ok_endpoint(request):
    return JSONResponse({"status": "ok"})

async def error_endpoint(request):
    return JSONResponse({"error": "not found"}, status_code=404)

async def crash_endpoint(request):
    raise RuntimeError("server error")


def make_test_app():
    app = Starlette(routes=[
        Route("/ok", ok_endpoint),
        Route("/error", error_endpoint),
        Route("/crash", crash_endpoint),
        Route("/metrics", lambda r: Response(content="", media_type="text/plain")),
    ])
    app.add_middleware(PrometheusMiddleware)
    return app


client = TestClient(make_test_app(), raise_server_exceptions=False)


# ── Tests normalize_path ──────────────────────────────────────────────────────

class TestNormalizePath:
    def test_chemin_simple(self):
        assert PrometheusMiddleware._normalize_path("/api/health") == "/api/health"

    def test_id_numerique_remplace(self):
        assert PrometheusMiddleware._normalize_path("/api/trips/42") == "/api/trips/{id}"

    def test_plusieurs_ids(self):
        assert PrometheusMiddleware._normalize_path("/api/trips/42/stop/7") == "/api/trips/{id}/stop/{id}"

    def test_chemin_sans_id(self):
        assert PrometheusMiddleware._normalize_path("/api/stats/overview") == "/api/stats/overview"

    def test_racine(self):
        assert PrometheusMiddleware._normalize_path("/") == "/"

    def test_segment_alpha_non_remplace(self):
        result = PrometheusMiddleware._normalize_path("/api/trains/TGV")
        assert "TGV" in result

    def test_segment_alphanum_non_remplace(self):
        result = PrometheusMiddleware._normalize_path("/api/trains/abc123")
        assert "abc123" in result


# ── Tests compteurs HTTP ──────────────────────────────────────────────────────

class TestPrometheusCounters:
    def test_requete_200_compte(self):
        before = REQUEST_COUNT.labels(method="GET", endpoint="/ok", status_code=200)._value.get()
        client.get("/ok")
        after = REQUEST_COUNT.labels(method="GET", endpoint="/ok", status_code=200)._value.get()
        assert after == before + 1

    def test_requete_404_compte(self):
        before = REQUEST_COUNT.labels(method="GET", endpoint="/error", status_code=404)._value.get()
        client.get("/error")
        after = REQUEST_COUNT.labels(method="GET", endpoint="/error", status_code=404)._value.get()
        assert after == before + 1

    def test_metrics_endpoint_pas_compte(self):
        before = REQUEST_COUNT.labels(method="GET", endpoint="/metrics", status_code=200)._value.get()
        client.get("/metrics")
        after = REQUEST_COUNT.labels(method="GET", endpoint="/metrics", status_code=200)._value.get()
        assert after == before


# ── Tests latence ─────────────────────────────────────────────────────────────

class TestPrometheusLatency:
    def test_latence_enregistree_apres_requete(self):
        before = REQUEST_LATENCY.labels(method="GET", endpoint="/ok")._sum.get()
        client.get("/ok")
        after = REQUEST_LATENCY.labels(method="GET", endpoint="/ok")._sum.get()
        assert after > before

    def test_latence_positive(self):
        client.get("/ok")
        total = REQUEST_LATENCY.labels(method="GET", endpoint="/ok")._sum.get()
        assert total >= 0


# ── Tests route /metrics de l'app principale ─────────────────────────────────

class TestMetricsEndpoint:
    def test_metrics_retourne_200(self):
        from main import app as main_app
        mc = TestClient(main_app)
        response = mc.get("/metrics")
        assert response.status_code == 200

    def test_metrics_content_type_prometheus(self):
        from main import app as main_app
        mc = TestClient(main_app)
        response = mc.get("/metrics")
        assert "text/plain" in response.headers.get("content-type", "")