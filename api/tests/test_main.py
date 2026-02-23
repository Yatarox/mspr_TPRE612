import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from main import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_root():
    response = client.get("/")
    assert response.status_code == 200