import pandas as pd
from transform_script.gtfs_time import parse_gtfs_time_to_sec, classifier_train, compute_durations

def test_parse_gtfs_time_to_sec():
    assert parse_gtfs_time_to_sec("12:34:56") == 45296
    assert parse_gtfs_time_to_sec("01:02") == 3720
    assert parse_gtfs_time_to_sec("23") == 82800
    assert parse_gtfs_time_to_sec("") is None
    assert parse_gtfs_time_to_sec(None) is None

def test_classifier_train():
    assert classifier_train("23:00:00") == "NUIT"
    assert classifier_train("05:59:00") == "NUIT"
    assert classifier_train("06:00:00") == "JOUR"
    assert classifier_train("21:59:00") == "JOUR"
    assert classifier_train("notatime") == "INCONNU"

def test_compute_durations():
    df = pd.DataFrame({
        "trip_id": ["t1", "t1", "t2", "t2"],
        "arrival_time": ["10:00:00", "11:00:00", "12:00:00", "13:00:00"],
        "departure_time": ["09:50:00", "11:05:00", "11:55:00", "13:05:00"],
        "stop_sequence": [1, 2, 1, 2]
    })
    durations = compute_durations(df)
    assert durations["t1"] == 70.0  # (11:00 - 09:50) = 70 min
    assert durations["t2"] == 65.0  # (13:00 - 11:55) = 65 min