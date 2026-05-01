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
            stop_country_map={},  
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
        assert all_rows[0]["distance_km"] > 300 

# ── gtfs_time ↔ gtfs_processing ──────────────────────────────────────────────

class TestTimeProcessingIntegration:

    def test_duration_computed_by_time_is_used_in_row(self, stops_paris_lyon, stop_times_paris_lyon, trips_tgv):
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

        expected_h = round(float(durations_min["T1"]) / 60.0, 2)
        assert all_rows[0]["duration_h"] == expected_h

    def test_classifier_train_called_with_real_departure_time(self, stops_paris_lyon, trips_tgv):
        night_stop_times = pd.DataFrame({
            "trip_id": ["T1", "T1"],
            "stop_id": ["A", "B"],
            "stop_sequence": [1, 2],
            "arrival_time":   ["23:00:00", "05:00:00"],
            "departure_time": ["23:10:00", "05:05:00"],
        })

        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(night_stop_times, stops_paris_lyon)
        durations_min = compute_durations(night_stop_times)

        first = night_stop_times.iloc[[0]].set_index("trip_id")
        last  = night_stop_times.iloc[[1]].set_index("trip_id")
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

        assert all_rows[0]["service_type"] == "NUIT"
        assert all_rows[0]["departure_time"] == "23:10:00"


# ── gtfs_emission ↔ gtfs_processing ──────────────────────────────────────────

class TestEmissionProcessingIntegration:

    def test_emission_values_match_direct_calculation(self, stops_paris_lyon, stop_times_paris_lyon, trips_tgv):
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

        row = all_rows[0]
        train_service = classify_train_service("101", "TGV", "SNCF", row["distance_km"], row["duration_h"])
        traction = estimate_traction("101", "TGV", "SNCF", train_service)
        expected_gco2e, expected_total = calculate_emissions(row["distance_km"], traction, train_service)

        assert row["emission_gco2e_pkm"] == round(expected_gco2e, 2)
        assert row["total_emission_kgco2e"] == round(expected_total, 3)
        assert row["traction"] == traction

    def test_diesel_regional_emission_pipeline(self, stops_paris_lyon):
        stop_times = pd.DataFrame({
            "trip_id": ["T1", "T1"],
            "stop_id": ["A", "B"],
            "stop_sequence": [1, 2],
            "arrival_time":   ["09:50:00", "11:00:00"],
            "departure_time": ["09:55:00", "11:05:00"],
        })
        trips_diesel = pd.DataFrame({
            "trip_id": ["T1"], "route_id": ["R1"], "service_id": ["S1"],
            "agency_name": ["AUTORAIL Sud"], "route_type": ["106"],
            "route_short_name": ["TER"], "route_long_name": ["AUTORAIL régional"],
            "monday": ["1"], "tuesday": ["0"], "wednesday": ["0"],
            "thursday": ["0"], "friday": ["0"], "saturday": ["0"], "sunday": ["0"],
        })

        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(stop_times, stops_paris_lyon)
        durations_min = compute_durations(stop_times)
        first = stop_times.iloc[[0]].set_index("trip_id")
        last  = stop_times.iloc[[1]].set_index("trip_id")
        freq_map = build_frequency_map(trips_diesel, first, last)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips_diesel,
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

        tgv_gco2e, _ = calculate_emissions(all_rows[0]["distance_km"], "électrique", "Grande vitesse")
        assert all_rows[0]["emission_gco2e_pkm"] > tgv_gco2e


# ── gtfs_frequency ↔ gtfs_processing ─────────────────────────────────────────

class TestFrequencyProcessingIntegration:

    def test_frequency_map_from_multiple_trips_same_route(self, stops_paris_lyon):
        """
        build_frequency_map compte 3 trips sur la même route/service →
        compute_frequency retourne 3 * jours_actifs.
        """
        stop_times = pd.DataFrame({
            "trip_id": ["T1","T1","T2","T2","T3","T3"],
            "stop_id": ["A","B","A","B","A","B"],
            "stop_sequence": [1,2,1,2,1,2],
            "arrival_time":   ["08:00:00","10:00:00","11:00:00","13:00:00","14:00:00","16:00:00"],
            "departure_time": ["08:05:00","10:05:00","11:05:00","13:05:00","14:05:00","16:05:00"],
        })
        trips = pd.DataFrame({
            "trip_id": ["T1", "T2", "T3"],
            "route_id": ["R1", "R1", "R1"],
            "service_id": ["S1", "S1", "S1"],
            "agency_name": ["SNCF", "SNCF", "SNCF"],
            "route_type": ["101", "101", "101"],
            "route_short_name": ["TGV", "TGV", "TGV"],
            "route_long_name": ["Paris-Lyon", "Paris-Lyon", "Paris-Lyon"],
            "monday":    ["1", "1", "1"],
            "tuesday":   ["1", "1", "1"],
            "wednesday": ["1", "1", "1"],
            "thursday":  ["1", "1", "1"],
            "friday":    ["1", "1", "1"],
            "saturday":  ["0", "0", "0"],
            "sunday":    ["0", "0", "0"],
        })

        first = stop_times[stop_times["stop_sequence"]=="1"].copy()
        first = stop_times.groupby("trip_id").first().reset_index().set_index("trip_id")
        last  = stop_times.groupby("trip_id").last().reset_index().set_index("trip_id")

        freq_map = build_frequency_map(trips, first, last)
        key = ("R1", "S1", "A", "B")
        assert freq_map[key] == 3

    
        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(stop_times, stops_paris_lyon)
        durations_min = compute_durations(stop_times)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips,
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

        for row in all_rows:
            assert row["frequency_per_week"] == 15

    def test_tous_les_jours_defaults_to_7_days(self, stops_paris_lyon, stop_times_paris_lyon):
        trips_no_days = pd.DataFrame({
            "trip_id": ["T1"], "route_id": ["R1"], "service_id": ["S1"],
            "agency_name": ["SNCF"], "route_type": ["101"],
            "route_short_name": ["TGV"], "route_long_name": ["Paris-Lyon"],
        })

        first = stop_times_paris_lyon.iloc[[0]].set_index("trip_id")
        last  = stop_times_paris_lyon.iloc[[1]].set_index("trip_id")
        freq_map = {("R1", "S1", "A", "B"): 2}
        stop_country_map = build_stop_country_map(stops_paris_lyon)
        distances_km = compute_distances(stop_times_paris_lyon, stops_paris_lyon)
        durations_min = compute_durations(stop_times_paris_lyon)
        all_rows = []

        _process_trips_chunk(
            trips_chunk=trips_no_days,
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

        assert all_rows[0]["frequency_per_week"] == 14

