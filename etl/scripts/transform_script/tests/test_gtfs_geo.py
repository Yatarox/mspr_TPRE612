import os
import sys
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_geo import (
    haversine_km,
    build_stop_country_map,
    extract_country_from_stop_name,
    compute_distances,
)


def test_haversine_km():
    # Paris (48.8566, 2.3522) -> Lyon (45.75, 4.85) ~ 392 km
    dist = haversine_km(48.8566, 2.3522, 45.75, 4.85)
    assert 390 < float(dist) < 400


def test_build_stop_country_map():
    df = pd.DataFrame(
        {
            "stop_id": ["1", "2"],
            "stop_lat": [48.85, 52.52],
            "stop_lon": [2.35, 13.40],
        }
    )
    country_map = build_stop_country_map(df)
    assert country_map["1"] == "FR"
    assert country_map["2"] == "DE"


def test_build_stop_country_map_empty_or_missing_cols():
    assert build_stop_country_map(pd.DataFrame()) == {}

    missing = pd.DataFrame({"stop_id": ["1"], "stop_lat": [48.85]})
    assert build_stop_country_map(missing) == {}


def test_build_stop_country_map_invalid_values_dropped():
    df = pd.DataFrame(
        {
            "stop_id": ["1", "2", None],
            "stop_lat": ["48.85", "not_a_number", 50.0],
            "stop_lon": ["2.35", "13.40", 2.0],
        }
    )
    country_map = build_stop_country_map(df)
    # seule la première ligne est valide
    assert country_map == {"1": "FR"}


def test_extract_country_from_stop_name():
    assert extract_country_from_stop_name("Paris Gare de Lyon") == "FR"
    assert extract_country_from_stop_name("Berlin Hauptbahnhof") == "DE"
    assert extract_country_from_stop_name("Unknown City") is None


def test_extract_country_from_stop_name_case_insensitive():
    assert extract_country_from_stop_name("lisbon oriente") == "PT"
    assert extract_country_from_stop_name("zürich hb") == "CH"


def test_compute_distances_empty_stop_times():
    stop_times = pd.DataFrame(columns=["trip_id", "stop_id", "stop_sequence"])
    stops = pd.DataFrame(columns=["stop_id", "stop_lat", "stop_lon"])

    out = compute_distances(stop_times, stops)
    assert out.empty


def test_compute_distances_with_shape_dist_traveled_km_and_meters():
    # t1: 0 -> 10 (km)
    # t2: 0 -> 2000 (meters => convert /1000 => 2km)
    stop_times = pd.DataFrame(
        {
            "trip_id": ["t1", "t1", "t2", "t2"],
            "stop_id": ["a", "b", "c", "d"],
            "stop_sequence": [1, 2, 1, 2],
            "shape_dist_traveled": [0, 10, 0, 2000],
        }
    )
    stops = pd.DataFrame({"stop_id": ["a", "b", "c", "d"], "stop_lat": [0, 0, 0, 0], "stop_lon": [0, 0, 0, 0]})

    out = compute_distances(stop_times, stops)

    assert out["t1"] == 10
    assert out["t2"] == 2


def test_compute_distances_without_shape_and_without_lat_lon():
    stop_times = pd.DataFrame(
        {
            "trip_id": ["t1", "t1"],
            "stop_id": ["a", "b"],
            "stop_sequence": [1, 2],
        }
    )
    stops = pd.DataFrame({"stop_id": ["a", "b"]})  # pas de stop_lat/stop_lon

    out = compute_distances(stop_times, stops)
    assert out.empty


def test_compute_distances_haversine_fallback():
    # 1 degré de longitude à l'équateur ~111 km
    stop_times = pd.DataFrame(
        {
            "trip_id": ["t1", "t1", "t2"],
            "stop_id": ["a", "b", "x"],  # t2 n'a qu'un arrêt -> pas de segment
            "stop_sequence": [1, 2, 1],
        }
    )
    stops = pd.DataFrame(
        {
            "stop_id": ["a", "b", "x"],
            "stop_lat": [0.0, 0.0, 10.0],
            "stop_lon": [0.0, 1.0, 10.0],
        }
    )

    out = compute_distances(stop_times, stops)

    assert "t1" in out.index
    assert 110 < out["t1"] < 112
    assert "t2" not in out.index


def test_build_stop_country_map_multiple_matches_and_outside():
    df = pd.DataFrame(
        {
            "stop_id": ["multi", "outside"],
            "stop_lat": [50.5, 0.0],
            "stop_lon": [3.0, 0.0],
        }
    )

    country_map = build_stop_country_map(df)

    assert country_map["multi"] == "FR"
    assert country_map["outside"] is None


def test_compute_distances_no_valid_segments_returns_empty():
    stop_times = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "stop_id": ["a"],
            "stop_sequence": [1],
        }
    )
    stops = pd.DataFrame(
        {
            "stop_id": ["a"],
            "stop_lat": [48.85],
            "stop_lon": [2.35],
        }
    )

    out = compute_distances(stop_times, stops)
    assert out.empty