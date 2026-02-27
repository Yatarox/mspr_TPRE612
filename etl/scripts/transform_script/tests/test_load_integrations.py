import pandas as pd
from transform_script.gtfs_geo import compute_distances
from transform_script.gtfs_time import compute_durations
from transform_script.gtfs_processing import _process_trips_chunk

def test_gtfs_processing_integration():
    # Simule des arrêts
    stops = pd.DataFrame({
        "stop_id": ["A", "B"],
        "stop_lat": [48.8566, 45.75],
        "stop_lon": [2.3522, 4.85]
    })

    # Simule des horaires de passage
    stop_times = pd.DataFrame({
        "trip_id": ["T1", "T1"],
        "stop_id": ["A", "B"],
        "stop_sequence": [1, 2],
        "arrival_time": ["10:00:00", "12:00:00"],
        "departure_time": ["09:50:00", "12:05:00"]
    })

    # Simule un trajet
    trips = pd.DataFrame({
        "trip_id": ["T1"],
        "agency_id": ["SNCF"],
        "agency_name": ["SNCF"],
        "route_id": ["R1"],
        "route_name": ["Paris-Lyon"],
        "route_type": ["101"],
        "service_id": ["S1"],
        "monday": ["1"],
        "tuesday": ["1"],
        "wednesday": ["1"],
        "thursday": ["1"],
        "friday": ["1"],
        "saturday": ["0"],
        "sunday": ["0"]
    })

    # Prépare les premiers et derniers arrêts
    first = stop_times.iloc[[0]].set_index("trip_id")
    last = stop_times.iloc[[1]].set_index("trip_id")

    stops_name = {"A": "Paris Gare de Lyon", "B": "Lyon Part-Dieu"}
    stop_country_map = {"A": "FR", "B": "FR"}

    # Calcule distances et durées
    distances_km = compute_distances(stop_times, stops)
    durations_min = compute_durations(stop_times)

    dataset_id_meta = "test"
    processed_dir = "/tmp"
    freq_map = {("R1", "S1", "A", "B"): 5}
    all_rows = []

    # Appelle la fonction de traitement
    count = _process_trips_chunk(
        trips, first, last, stops_name, stop_country_map,
        distances_km, durations_min, dataset_id_meta, processed_dir,
        freq_map, all_rows
    )

    # Vérifie le résultat
    assert count == 1
    assert all_rows[0]["origin_country"] == "FR"
    assert all_rows[0]["destination_country"] == "FR"
    assert all_rows[0]["distance_km"] > 0
    assert all_rows[0]["frequency_per_week"] == 25
    assert all_rows[0]["origin_stop_name"] == "Paris Gare de Lyon"
    assert all_rows[0]["destination_stop_name"] == "Lyon Part-Dieu"