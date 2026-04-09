import os
import sys

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_time import (
    parse_gtfs_time_to_sec,
    classifier_train,
    compute_durations,
)


def test_parse_gtfs_time_to_sec():
    assert parse_gtfs_time_to_sec("12:34:56") == 45296
    assert parse_gtfs_time_to_sec("01:02") == 3720
    assert parse_gtfs_time_to_sec("23") == 82800
    assert parse_gtfs_time_to_sec("") is None
    assert parse_gtfs_time_to_sec(None) is None
    assert parse_gtfs_time_to_sec("notatime") is None
    assert parse_gtfs_time_to_sec("12:xx:00") is None


def test_classifier_train():
    assert classifier_train("23:00:00") == "NUIT"
    assert classifier_train("05:59:00") == "NUIT"
    assert classifier_train("06:00:00") == "JOUR"
    assert classifier_train("21:59:00") == "JOUR"
    assert classifier_train("notatime") == "INCONNU"
    assert classifier_train("") == "INCONNU"
    assert classifier_train(None) == "INCONNU"


def test_compute_durations():
    df = pd.DataFrame(
        {
            "trip_id": ["t1", "t1", "t2", "t2"],
            "arrival_time": ["10:00:00", "11:00:00", "12:00:00", "13:00:00"],
            "departure_time": ["09:50:00", "11:05:00", "11:55:00", "13:05:00"],
            "stop_sequence": [1, 2, 1, 2],
        }
    )
    durations = compute_durations(df)
    assert durations["t1"] == 70.0
    assert durations["t2"] == 65.0


def test_compute_durations_empty_df():
    out = compute_durations(pd.DataFrame())
    assert out.empty


def test_compute_durations_missing_columns():
    df = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "arrival_time": ["10:00:00"],
        }
    )
    out = compute_durations(df)
    assert out.empty


def test_compute_durations_negative_duration_uses_alt():
    df = pd.DataFrame(
        {
            "trip_id": ["t1", "t1"],
            "arrival_time": ["08:00:00", "07:00:00"],
            "departure_time": ["09:00:00", "10:00:00"],
            "stop_sequence": [1, 2],
        }
    )
    durations = compute_durations(df)
    assert durations["t1"] == 180.0


def test_compute_durations_single_valid_timestamp_returns_zero():
    df = pd.DataFrame(
        {
            "trip_id": ["t1"],
            "arrival_time": [None],
            "departure_time": ["09:00:00"],
            "stop_sequence": [1],
        }
    )
    durations = compute_durations(df)
    assert durations["t1"] == 0.0