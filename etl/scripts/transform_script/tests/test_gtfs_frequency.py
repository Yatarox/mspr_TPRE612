import os
import sys
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_frequency import (
    build_frequency_map,
    compute_frequency,
    calculate_frequency_per_week_intermediate,
)


def test_build_frequency_map_counts_and_dropna():
    trips = pd.DataFrame(
        {
            "trip_id": ["t1", "t2", "t3", "t4"],
            "route_id": ["r1", "r1", "r2", "r2"],
            "service_id": ["s1", "s1", "s2", "s2"],
        }
    )

    first = pd.DataFrame({"stop_id": {"t1": "A", "t2": "A", "t3": "B"}})
    last = pd.DataFrame({"stop_id": {"t1": "Z", "t2": "Z", "t3": "Y"}})
    # t4 absent de first/last -> dropna

    freq_map = build_frequency_map(trips, first, last)

    assert freq_map[("r1", "s1", "A", "Z")] == 2
    assert freq_map[("r2", "s2", "B", "Y")] == 1
    assert len(freq_map) == 2


def test_build_frequency_map_casts_to_string():
    trips = pd.DataFrame(
        {
            "trip_id": [1, 2],
            "route_id": [10, 10],
            "service_id": [100, 100],
        }
    )

    first = pd.DataFrame({"stop_id": {1: 111, 2: 111}})
    last = pd.DataFrame({"stop_id": {1: 999, 2: 999}})

    freq_map = build_frequency_map(trips, first, last)

    assert ("10", "100", "111", "999") in freq_map
    assert freq_map[("10", "100", "111", "999")] == 2


def test_compute_frequency_default_when_key_missing():
    freq_map = {}
    key = ("r", "s", "o", "d")

    assert compute_frequency(5, key, freq_map) == 5


def test_compute_frequency_min_one_when_zero_or_negative():
    key = ("r", "s", "o", "d")

    assert compute_frequency(3, key, {key: 0}) == 3
    assert compute_frequency(3, key, {key: -4}) == 3


def test_compute_frequency_caps_to_20():
    key = ("r", "s", "o", "d")
    freq_map = {key: 50}

    assert compute_frequency(7, key, freq_map) == 140 


def test_calculate_frequency_per_week_intermediate_default_7_days():
    key = ("r", "s", "o", "d")
    freq_map = {key: 3}

    assert calculate_frequency_per_week_intermediate("", key, freq_map) == 21
    assert (
        calculate_frequency_per_week_intermediate("Tous les jours", key, freq_map) == 21
    )


def test_calculate_frequency_per_week_intermediate_split_days():
    key = ("r", "s", "o", "d")
    freq_map = {key: 2}

    assert (
        calculate_frequency_per_week_intermediate("Lundi,Mardi,Mercredi", key, freq_map)
        == 6
    )