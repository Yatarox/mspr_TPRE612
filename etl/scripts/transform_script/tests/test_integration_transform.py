import os
import sys
import json
import pandas as pd
import numpy as np
import pytest

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_geo import (
    build_stop_country_map,
    compute_distances,
    extract_country_from_stop_name,
)
from transform_script.gtfs_time import compute_durations, classifier_train
from transform_script.gtfs_frequency import build_frequency_map, calculate_frequency_per_week_intermediate
from transform_script.gtfs_emission import estimate_traction, calculate_emissions
from transform_script.gtfs_processing import _process_trips_chunk, classify_train_service
from transform_script.gtfs_helpers import read_csv,read_metadata


@pytest.fixture
def stops_paris_lyon():
    return pd.DataFrame({
        "stop_id": ["A", "B"],
        "stop_lat": [48.8566, 45.75],
        "stop_lon": [2.3522, 4.85],
        "stop_name": ["Paris Gare de Lyon", "Lyon Part-Dieu"],
    })

@pytest.fixture
def stop_times_paris_lyon():
    return pd.DataFrame({
        "trip_id": ["T1", "T1"],
        "stop_id": ["A", "B"],
        "stop_sequence": [1, 2],
        "arrival_time":   ["09:50:00", "12:00:00"],
        "departure_time": ["09:55:00", "12:05:00"],
    })

@pytest.fixture
def trips_tgv():
    return pd.DataFrame({
        "trip_id":    ["T1"],
        "route_id":   ["R1"],
        "service_id": ["S1"],
        "agency_name": ["SNCF"],
        "route_type": ["101"],
        "route_short_name": ["TGV"],
        "route_long_name":  ["Paris-Lyon"],
        "monday": ["1"], "tuesday": ["1"], "wednesday": ["1"],
        "thursday": ["1"], "friday": ["1"], "saturday": ["0"], "sunday": ["0"],
    })


# ── gtfs_geo ↔ gtfs_processing ────────────────────────────────────────────────

class TestGeoProcessingIntegration:

    def test_country_map_used_in_process_chunk(self, stops_paris_lyon, stop_times_paris_lyon, trips_tgv):
        """
        build_stop_country_map produit une map → _process_trips_chunk l'utilise
        pour assigner les pays sans appeler extract_country_from_stop_name.
        """
        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(stop_times_paris_lyon, stops_paris_lyon)
        durations_min = compute_durations(stop_times_paris_lyon)

        first = stop_times_paris_lyon.iloc[[0]].set_index("trip_id")
        last  = stop_times_paris_lyon.iloc[[1]].set_index("trip_id")

        freq_map = build_frequency_map(trips_tgv, first, last)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips_tgv,
            first=first, last=last,
            stops_name={"A": "Paris Gare de Lyon", "B": "Lyon Part-Dieu"},
            stop_country_map=stop_country_map,
            distances_km=distances_km,
            durations_min=durations_min,
            dataset_id_meta="ds-test",
            processed_dir="/tmp",
            freq_map=freq_map,
            all_rows=all_rows,
        )

        assert len(all_rows) == 1
        assert all_rows[0]["origin_country"] == "FR"
        assert all_rows[0]["destination_country"] == "FR"

    def test_fallback_to_stop_name_when_no_country_map(self, stops_paris_lyon, stop_times_paris_lyon, trips_tgv):
        distances_km = compute_distances(stop_times_paris_lyon, stops_paris_lyon)
        durations_min = compute_durations(stop_times_paris_lyon)

        first = stop_times_paris_lyon.iloc[[0]].set_index("trip_id")
        last  = stop_times_paris_lyon.iloc[[1]].set_index("trip_id")
        freq_map = build_frequency_map(trips_tgv, first, last)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips_tgv,
            first=first, last=last,
            stops_name={"A": "Paris Gare de Lyon", "B": "Lyon Part-Dieu"},
            stop_country_map={},  # vide → fallback sur stop_name
            distances_km=distances_km,
            durations_min=durations_min,
            dataset_id_meta="ds-test",
            processed_dir="/tmp",
            freq_map=freq_map,
            all_rows=all_rows,
        )

        assert all_rows[0]["origin_country"] == "FR"
        assert all_rows[0]["destination_country"] == "FR"

    def test_distance_computed_by_geo_is_used_in_row(self, stops_paris_lyon, stop_times_paris_lyon, trips_tgv):
        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(stop_times_paris_lyon, stops_paris_lyon)
        durations_min = compute_durations(stop_times_paris_lyon)

        first = stop_times_paris_lyon.iloc[[0]].set_index("trip_id")
        last  = stop_times_paris_lyon.iloc[[1]].set_index("trip_id")
        freq_map = build_frequency_map(trips_tgv, first, last)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips_tgv,
            first=first, last=last,
            stops_name={"A": "Paris Gare de Lyon", "B": "Lyon Part-Dieu"},
            stop_country_map=stop_country_map,
            distances_km=distances_km,
            durations_min=durations_min,
            dataset_id_meta="ds-test",
            processed_dir="/tmp",
            freq_map=freq_map,
            all_rows=all_rows,
        )

        expected_dist = float(distances_km["T1"])
        assert all_rows[0]["distance_km"] == round(expected_dist, 3)
        assert all_rows[0]["distance_km"] > 300  # Paris-Lyon ~390 km

