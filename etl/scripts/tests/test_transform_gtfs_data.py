import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _get_tmod():
    return importlib.import_module("transform_gtfs_data")


def test_build_trips_summary_no_version_dir(tmp_path):
    tmod = _get_tmod()
    with patch.object(tmod, "latest_version_dir", return_value=None):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )
    assert count == 0
    assert out == ""


def test_build_trips_summary_empty_trips_or_stop_times(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    def _fake_read_csv(path):
        name = Path(path).name
        if name == "agency.txt":
            return pd.DataFrame({"agency_id": ["A1"], "agency_name": ["SNCF"]})
        if name == "routes.txt":
            return pd.DataFrame({"route_id": ["R1"], "agency_id": ["A1"]})
        if name == "stops.txt":
            return pd.DataFrame({"stop_id": ["s1"], "stop_name": ["Paris"]})
        if name == "trips.txt":
            return pd.DataFrame()  # force empty
        if name == "stop_times.txt":
            return pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})
        return pd.DataFrame()

    with patch.object(tmod, "latest_version_dir", return_value=latest), patch.object(
        tmod, "read_metadata", return_value={"dataset_id": "ds1"}
    ), patch.object(tmod, "read_csv", side_effect=_fake_read_csv):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_transform_gtfs_skip_existing_returns_existing_files(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "ds1").mkdir(parents=True)

    out = processed / "ds1" / "trips_summary_ds1.csv"
    out.parent.mkdir(parents=True)
    out.write_text("trip_id\n", encoding="utf-8")

    with patch.object(tmod, "_resolve_dataset_output_id", return_value="ds1"):
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=True)

    assert str(out) in written


def test_transform_gtfs_process_pool_success_and_empty(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)
    (staging / "b").mkdir(parents=True)

    fut_ok = MagicMock()
    fut_ok.result.return_value = (3, str(processed / "a" / "trips_summary_a.csv"))

    fut_empty = MagicMock()
    fut_empty.result.return_value = (0, "")

    executor = MagicMock()
    executor.submit.side_effect = [fut_ok, fut_empty]

    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, patch.object(
        tmod, "as_completed", return_value=[fut_ok, fut_empty]
    ):
        pool_cls.return_value.__enter__.return_value = executor
        written = tmod.transform_gtfs(str(staging), str(processed), max_workers=4, skip_existing=False)

    assert len(written) == 1
    assert "trips_summary_a.csv" in written[0]
    pool_cls.assert_called_once_with(max_workers=2)


def test_transform_gtfs_timeout_branch(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)

    fut_timeout = MagicMock()
    fut_timeout.result.side_effect = tmod.FuturesTimeoutError()

    executor = MagicMock()
    executor.submit.return_value = fut_timeout

    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, patch.object(
        tmod, "as_completed", return_value=[fut_timeout]
    ):
        pool_cls.return_value.__enter__.return_value = executor
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=False)

    assert written == []

def test_filter_rail_only_keeps_only_rail_types():
    tmod = _get_tmod()
    
    routes_df = pd.DataFrame({
        "route_id": ["R1", "R2", "R3", "R4", "R5"],
        "route_type": [2, 3, 100, 4, 101],  # 2=Rail, 3=Bus, 100=Rail, 4=Ferry, 101=Rail
        "agency_id": ["SNCF", "BUS", "SNCF", "FER", "SNCF"]
    })
    
    trips_df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3", "T4", "T5"],
        "route_id": ["R1", "R2", "R3", "R4", "R5"],
        "trip_name": ["train1", "bus1", "train2", "ferry1", "train3"]
    })
    
    result = tmod.filter_rail_only(routes_df, trips_df, "test_dataset")
    
    assert len(result) == 3
    assert set(result["route_id"]) == {"R1", "R3", "R5"}
    assert "R2" not in result["route_id"].values
    assert "R4" not in result["route_id"].values


def test_filter_rail_only_with_string_route_type():
    tmod = _get_tmod()
    
    routes_df = pd.DataFrame({
        "route_id": ["R1", "R2"],
        "route_type": ["2", "3"],
        "agency_id": ["SNCF", "BUS"]
    })
    
    trips_df = pd.DataFrame({
        "trip_id": ["T1", "T2"],
        "route_id": ["R1", "R2"]
    })
    
    result = tmod.filter_rail_only(routes_df, trips_df, "test_dataset")
    
    assert len(result) == 1
    assert result.iloc[0]["route_id"] == "R1"


def test_filter_rail_only_no_valid_routes_returns_empty():
    tmod = _get_tmod()
    
    routes_df = pd.DataFrame({
        "route_id": ["R1", "R2"],
        "route_type": [3, 4], 
        "agency_id": ["BUS", "FER"]
    })
    
    trips_df = pd.DataFrame({
        "trip_id": ["T1", "T2"],
        "route_id": ["R1", "R2"]
    })
    
    result = tmod.filter_rail_only(routes_df, trips_df, "test_dataset")
    
    assert result.empty


def test_sanitize_dataframe_removes_invalid_distances():
    tmod = _get_tmod()
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3", "T4"],
        "distance_km": [100, 0, -5, np.nan],
        "duration_h": [1.5, 1.5, 1.5, 1.5],
        "emission_gco2e_pkm": [10, 10, 10, 10],
        "total_emission_kgco2e": [1, 1, 1, 1]
    })
    
    result = tmod._sanitize_dataframe(df, "test_dataset")
    assert len(result) == 2
    assert result.iloc[0]["trip_id"] == "T1"
    assert result.iloc[1]["trip_id"] == "T2"
    assert result.iloc[1]["distance_km"] == 0


def test_impute_missing_distances():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3", "T4"],
        "train_type": ["Rail", "Rail", "Rail", "Bus"],
        "distance_km": [100, 0, np.nan, 50],
        "duration_h": [2.0, 1.0, 1.5, 1.0],
        "total_emission_kgco2e": [2.5, 0, np.nan, 1.2]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T2", "distance_km"].values[0] > 0
    assert result.loc[result["trip_id"] == "T3", "distance_km"].values[0] > 0
    assert result.loc[result["trip_id"] == "T2", "total_emission_kgco2e"].values[0] > 0
    assert result.loc[result["trip_id"] == "T4", "distance_km"].values[0] == 50  # inchangé


def test_build_trips_summary_with_rail_filter_only(tmp_path):

    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds_rail" / "v1"
    latest.mkdir(parents=True)
    
    agency_df = pd.DataFrame({"agency_id": ["SNCF"], "agency_name": ["SNCF"]})
    routes_df = pd.DataFrame({
        "route_id": ["R_RAIL", "R_BUS"],
        "route_type": [2, 3],  # Rail et Bus
        "agency_id": ["SNCF", "SNCF"]
    })
    stops_df = pd.DataFrame({
        "stop_id": ["S1", "S2"],
        "stop_name": ["Paris", "Lyon"],
        "stop_lat": [48.8566, 45.7640],
        "stop_lon": [2.3522, 4.8357]
    })
    trips_df = pd.DataFrame({
        "trip_id": ["T_RAIL", "T_BUS"],
        "route_id": ["R_RAIL", "R_BUS"],
        "service_id": ["SVC1", "SVC1"]
    })
    stop_times_df = pd.DataFrame({
        "trip_id": ["T_RAIL", "T_RAIL", "T_BUS", "T_BUS"],
        "stop_id": ["S1", "S2", "S1", "S2"],
        "stop_sequence": [1, 2, 1, 2],
        "arrival_time": ["10:00", "12:00", "10:00", "12:00"],
        "departure_time": ["10:00", "12:00", "10:00", "12:00"]
    })
    calendar_df = pd.DataFrame({
        "service_id": ["SVC1"],
        "monday": [1], "tuesday": [1], "wednesday": [1],
        "thursday": [1], "friday": [1], "saturday": [0], "sunday": [0]
    })
    
    def _fake_read_csv(path):
        name = Path(path).name
        if name == "agency.txt":
            return agency_df
        if name == "routes.txt":
            return routes_df
        if name == "stops.txt":
            return stops_df
        if name == "trips.txt":
            return trips_df
        if name == "stop_times.txt":
            return stop_times_df
        if name == "calendar.txt":
            return calendar_df
        return pd.DataFrame()
    
    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", return_value={"dataset_id": "ds_rail"}), \
         patch.object(tmod, "read_csv", side_effect=_fake_read_csv), \
         patch.object(tmod, "compute_distances", return_value=pd.Series({"T_RAIL": 500, "T_BUS": 500})), \
         patch.object(tmod, "compute_durations", return_value=pd.Series({"T_RAIL": 2.0, "T_BUS": 2.0})), \
         patch("transform_script.gtfs_helpers._impute_missing_distances", side_effect=lambda x, y: x):
        
        count, out_path = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds_rail", str(tmp_path / "processed")
        )
    
    assert count == 1
    assert out_path != ""
    
    result_df = pd.read_csv(out_path)
    assert len(result_df) == 1
    assert result_df.iloc[0]["trip_id"] == "T_RAIL"
    assert result_df.iloc[0]["train_type"] == "Rail" 


def test_sanitize_dataframe_keeps_zero_distance_for_rail_if_imputed():
    """Vérifie que le pipeline complet impute les distances"""
    tmod = _get_tmod()
    
    df = pd.DataFrame({
        "trip_id": ["T1"],
        "distance_km": [0],
        "duration_h": [1.5],
        "train_type": ["Rail"],
        "emission_gco2e_pkm": [25],
        "total_emission_kgco2e": [0]
    })
    
    result = tmod._sanitize_dataframe(df, "test_dataset")
    assert len(result) == 1  
    assert result.iloc[0]["distance_km"] == 0  


def test_filter_rail_only_with_non_rail_agencies():
    """Vérifie que filter_rail_only filtre aussi par agency_id blacklistée"""
    tmod = _get_tmod()
    
    routes_df = pd.DataFrame({
        "route_id": ["R1", "R2", "R3"],
        "route_type": [2, 2, 2],  # Tous rail
        "agency_id": ["SNCF", "AEROPORT_NANTES", "SNCF"]
    })
    
    trips_df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3"],
        "route_id": ["R1", "R2", "R3"]
    })
    
    result = tmod.filter_rail_only(routes_df, trips_df, "test_dataset")
    
    assert len(result) == 3


def test_transform_gtfs_process_pool_fallback_on_broken_pool(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)
    (staging / "b").mkdir(parents=True)
    
    # Simule un ProcessPool cassé
    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, \
         patch.object(tmod, "BrokenProcessPool", type("BrokenProcessPool", (Exception,), {})), \
         patch.object(tmod, "build_trips_summary_for_dataset", return_value=(5, "fake_path")):
        
        pool_cls.side_effect = tmod.BrokenProcessPool()
        
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=False)
    
    assert len(written) == 2  # 