import pandas as pd
from transform_script.gtfs_geo import haversine_km, build_stop_country_map, extract_country_from_stop_name

def test_haversine_km():
    # Paris (48.8566, 2.3522) to Lyon (45.75, 4.85) ~ 392 km
    dist = haversine_km(48.8566, 2.3522, 45.75, 4.85)
    assert 390 < dist < 400

def test_build_stop_country_map():
    df = pd.DataFrame({
        "stop_id": ["1", "2"],
        "stop_lat": [48.85, 52.52],
        "stop_lon": [2.35, 13.40]
    })
    country_map = build_stop_country_map(df)
    assert country_map["1"] == "FR"
    assert country_map["2"] is None or country_map["2"] == "DE"

def test_extract_country_from_stop_name():
    assert extract_country_from_stop_name("Paris Gare de Lyon") == "FR"
    assert extract_country_from_stop_name("Berlin Hauptbahnhof") == "DE"
    assert extract_country_from_stop_name("Unknown City") is None