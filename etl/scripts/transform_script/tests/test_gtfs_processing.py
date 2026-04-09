import os
import sys
from unittest.mock import patch

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script import gtfs_processing as mod


def test_route_title_fallback_origin_destination():
    row = {"route_short_name": "---/---", "route_long_name": "---/---"}
    out = mod._route_title(row, "Paris", "Lyon")
    assert out == "Paris - Lyon"


def test_route_title_short_priority_when_both_present():
    row = {"route_short_name": "TGV 1", "route_long_name": "Paris Lyon"}
    out = mod._route_title(row, "Paris", "Lyon")
    assert out == "TGV 1"


def test_split_by_agency_without_column():
    trips = pd.DataFrame({"trip_id": ["t1", "t2"]})
    out = mod.split_by_agency(trips)
    assert list(out.keys()) == ["all"]
    assert out["all"].equals(trips)


def test_split_by_agency_with_values_and_empty():
    trips = pd.DataFrame(
        {
            "trip_id": ["t1", "t2", "t3"],
            "agency_id": ["A", "", "B"],
        }
    )
    out = mod.split_by_agency(trips)
    assert set(out.keys()) == {"A", "B"}
    assert len(out["A"]) == 1
    assert len(out["B"]) == 1


def test_classify_train_service_route_type_map():
    assert mod.classify_train_service("101", "", "", 0, 0) == "Grande vitesse"
    assert mod.classify_train_service("106", "", "", 0, 0) == "Régional"


def test_classify_train_service_keywords_and_thresholds():
    assert mod.classify_train_service("X", "TGV INOUI", "SNCF", 0, 0) == "Grande vitesse"
    assert mod.classify_train_service("X", "Intercity", "X", 0, 0) == "Intercité"
    assert mod.classify_train_service("X", "TER", "X", 0, 0) == "Régional"
    assert mod.classify_train_service("X", "Nightjet", "X", 0, 0) == "International"
    assert mod.classify_train_service("X", "", "", 900, 1) == "Grande ligne"
    assert mod.classify_train_service("X", "", "", 300, 1) == "Intercité"
    assert mod.classify_train_service("X", "", "", 50, 1) == "Régional"
    assert mod.classify_train_service("X", "", "", 0, 0) == "Inconnu"


@patch("transform_script.gtfs_processing.calculate_frequency_per_week_intermediate", return_value=14)
@patch("transform_script.gtfs_processing.estimate_traction", return_value="électrique")
@patch("transform_script.gtfs_processing.calculate_emissions", return_value=(3.2, 0.32))
@patch("transform_script.gtfs_processing.is_valid_numeric", return_value=True)
@patch("transform_script.gtfs_processing.classifier_train", return_value="MATIN")
def test_process_trips_chunk_happy_path(
    _mock_classifier,
    _mock_valid,
    _mock_emissions,
    _mock_traction,
    _mock_freq,
):
    trips_chunk = pd.DataFrame(
        [
            {
                "trip_id": "t1",
                "agency_name": "SNCF",
                "route_type": "101",
                "route_id": "r1",
                "service_id": "s1",
                "route_short_name": "TGV",
                "route_long_name": "Paris-Lyon",
                "monday": "1",
                "tuesday": "1",
                "wednesday": "0",
                "thursday": "0",
                "friday": "0",
                "saturday": "0",
                "sunday": "0",
            }
        ]
    )

    first = pd.DataFrame(
        {"stop_id": {"t1": "A"}, "departure_time": {"t1": "08:00:00"}}
    )
    last = pd.DataFrame(
        {"stop_id": {"t1": "B"}, "arrival_time": {"t1": "10:00:00"}}
    )

    stops_name = {"A": "Paris", "B": "Lyon"}
    stop_country_map = {"A": "FR", "B": "FR"}
    distances_km = pd.Series({"t1": 100.0})
    durations_min = pd.Series({"t1": 120.0})
    freq_map = {("r1", "s1", "A", "B"): 2}
    all_rows = []

    processed = mod._process_trips_chunk(
        trips_chunk=trips_chunk,
        first=first,
        last=last,
        stops_name=stops_name,
        stop_country_map=stop_country_map,
        distances_km=distances_km,
        durations_min=durations_min,
        dataset_id_meta="ds1",
        processed_dir="unused",
        freq_map=freq_map,
        all_rows=all_rows,
    )

    assert processed == 1
    assert len(all_rows) == 1
    row = all_rows[0]
    assert row["trip_id"] == "t1"
    assert row["origin_stop_name"] == "Paris"
    assert row["destination_stop_name"] == "Lyon"
    assert row["frequency_per_week"] == 14
    assert row["service_type"] == "MATIN"
    assert row["traction"] == "électrique"


@patch("transform_script.gtfs_processing.calculate_frequency_per_week_intermediate", return_value=7)
@patch("transform_script.gtfs_processing.estimate_traction", return_value="diesel")
@patch("transform_script.gtfs_processing.calculate_emissions", return_value=("NaN*", 1.0))
@patch("transform_script.gtfs_processing.is_valid_numeric", return_value=False)
def test_process_trips_chunk_skips_invalid_emission(
    _mock_valid, _mock_emissions, _mock_traction, _mock_freq
):
    trips_chunk = pd.DataFrame(
        [
            {
                "trip_id": "t1",
                "agency_name": "SNCF",
                "route_type": "2",
                "route_id": "r1",
                "service_id": "s1",
                "route_short_name": "X",
                "route_long_name": "Y",
            }
        ]
    )

    first = pd.DataFrame(
        {"stop_id": {"t1": "A"}, "departure_time": {"t1": "08:00:00"}}
    )
    last = pd.DataFrame(
        {"stop_id": {"t1": "B"}, "arrival_time": {"t1": "09:00:00"}}
    )

    all_rows = []
    processed = mod._process_trips_chunk(
        trips_chunk=trips_chunk,
        first=first,
        last=last,
        stops_name={"A": "Paris", "B": "Lyon"},
        stop_country_map={"A": "FR", "B": "FR"},
        distances_km=pd.Series({"t1": 10.0}),
        durations_min=pd.Series({"t1": 60.0}),
        dataset_id_meta="ds1",
        processed_dir="unused",
        freq_map={("r1", "s1", "A", "B"): 1},
        all_rows=all_rows,
    )

    assert processed == -1  # len(all_rows)=0, skipped_invalid=1 (comportement actuel)
    assert all_rows == []


def test_process_trips_chunk_skips_when_trip_not_in_first_last():
    trips_chunk = pd.DataFrame([{"trip_id": "missing"}])

    first = pd.DataFrame({"stop_id": {"t1": "A"}, "departure_time": {"t1": "08:00:00"}})
    last = pd.DataFrame({"stop_id": {"t1": "B"}, "arrival_time": {"t1": "10:00:00"}})

    all_rows = []
    processed = mod._process_trips_chunk(
        trips_chunk=trips_chunk,
        first=first,
        last=last,
        stops_name={},
        stop_country_map={},
        distances_km=pd.Series(dtype=float),
        durations_min=pd.Series(dtype=float),
        dataset_id_meta="ds1",
        processed_dir="unused",
        freq_map={},
        all_rows=all_rows,
    )

    assert processed == 0
    assert all_rows == []


def test_classify_train_service_default_branch():
    assert mod.classify_train_service("999", "", "", 0, 0) == "Inconnu"


@patch("transform_script.gtfs_processing.is_valid_numeric", return_value=False)
@patch("transform_script.gtfs_processing.calculate_frequency_per_week_intermediate", return_value=7)
@patch("transform_script.gtfs_processing.estimate_traction", return_value="mixte")
@patch("transform_script.gtfs_processing.calculate_emissions", return_value=(0.0, 0.0))
def test_process_trips_chunk_skips_invalid_numeric(
    _mock_emissions,
    _mock_traction,
    _mock_freq,
    _mock_valid,
):
    trips_chunk = pd.DataFrame(
        [
            {
                "trip_id": "t1",
                "agency_name": "SNCF",
                "route_type": "999",
                "route_id": "r1",
                "service_id": "s1",
                "route_short_name": "X",
                "route_long_name": "Y",
            }
        ]
    )

    first = pd.DataFrame({"stop_id": {"t1": "A"}, "departure_time": {"t1": "08:00:00"}})
    last = pd.DataFrame({"stop_id": {"t1": "B"}, "arrival_time": {"t1": "09:00:00"}})

    all_rows = []
    processed = mod._process_trips_chunk(
        trips_chunk=trips_chunk,
        first=first,
        last=last,
        stops_name={"A": "Paris", "B": "Lyon"},
        stop_country_map={"A": "FR", "B": "FR"},
        distances_km=pd.Series({"t1": 10.0}),
        durations_min=pd.Series({"t1": 60.0}),
        dataset_id_meta="ds1",
        processed_dir="unused",
        freq_map={("r1", "s1", "A", "B"): 1},
        all_rows=all_rows,
    )

    assert processed == 0 or processed == -1
    assert all_rows == []

def test_route_title_long_only_fallback():
    row = {"route_short_name": "", "route_long_name": "Paris-Lyon"}
    assert mod._route_title(row, "Paris", "Lyon") == "Paris-Lyon"


@patch("transform_script.gtfs_processing.extract_country_from_stop_name", side_effect=["FR", "DE"])
@patch("transform_script.gtfs_processing.calculate_frequency_per_week_intermediate", return_value=7)
@patch("transform_script.gtfs_processing.estimate_traction", return_value="mixte")
@patch("transform_script.gtfs_processing.calculate_emissions", return_value=(3.2, 0.32))
@patch("transform_script.gtfs_processing.is_valid_numeric", return_value=True)
@patch("transform_script.gtfs_processing.classifier_train", return_value="MATIN")
def test_process_trips_chunk_infers_countries_from_stop_names(
    _mock_classifier,
    _mock_valid,
    _mock_emissions,
    _mock_traction,
    _mock_freq,
    _mock_extract_country,
):
    trips_chunk = pd.DataFrame(
        [
            {
                "trip_id": "t1",
                "agency_name": "SNCF",
                "route_type": "999",
                "route_id": "r1",
                "service_id": "s1",
                "route_short_name": "",
                "route_long_name": "Paris-Berlin",
            }
        ]
    )

    first = pd.DataFrame(
        {"stop_id": {"t1": "A"}, "departure_time": {"t1": "08:00:00"}}
    )
    last = pd.DataFrame(
        {"stop_id": {"t1": "B"}, "arrival_time": {"t1": "10:00:00"}}
    )

    all_rows = []
    processed = mod._process_trips_chunk(
        trips_chunk=trips_chunk,
        first=first,
        last=last,
        stops_name={"A": "Paris Gare", "B": "Berlin Hbf"},
        stop_country_map={}, 
        distances_km=pd.Series({"t1": 100.0}),
        durations_min=pd.Series({"t1": 120.0}),
        dataset_id_meta="ds1",
        processed_dir="unused",
        freq_map={("r1", "s1", "A", "B"): 1},
        all_rows=all_rows,
    )

    assert processed == 1
    assert len(all_rows) == 1
    assert all_rows[0]["origin_country"] == "FR"
    assert all_rows[0]["destination_country"] == "DE"