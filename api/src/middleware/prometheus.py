import time
from prometheus_client import Counter, Histogram, Gauge
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Nombre total de requêtes HTTP",
    ["method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Latence des requêtes HTTP en secondes",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Requêtes HTTP en cours de traitement",
    ["method", "endpoint"],
)

PREDICTION_COUNT = Counter(
    "co2_predictions_total",
    "Nombre total de prédictions CO2",
    ["status"],  # success | error
)

PREDICTION_LATENCY = Histogram(
    "co2_prediction_duration_seconds",
    "Latence des prédictions CO2 en secondes",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

PREDICTION_VALUE = Histogram(
    "co2_prediction_value_gco2e_pkm",
    "Distribution des valeurs prédites (gCO2e/km)",
    buckets=[0, 5, 10, 20, 30, 50, 75, 100, 150, 200],
)



class PrometheusMiddleware(BaseHTTPMiddleware):
    EXCLUDE_PATHS = {"/metrics", "/favicon.ico"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if path in self.EXCLUDE_PATHS:
            return await call_next(request)

        method = request.method
        endpoint = self._normalize_path(path)

        REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).inc()
        start = time.perf_counter()

        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as exc:
            status_code = 500
            REQUEST_COUNT.labels(method=method, endpoint=endpoint, status_code=status_code).inc()
            REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()
            raise exc
        finally:
            duration = time.perf_counter() - start
            REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(duration)
            REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()

        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status_code=status_code).inc()
        return response

    @staticmethod
    def _normalize_path(path: str) -> str:
        parts = path.split("/")
        normalized = []
        for part in parts:
            if part.isdigit():
                normalized.append("{id}")
            else:
                normalized.append(part)
        return "/".join(normalized)
