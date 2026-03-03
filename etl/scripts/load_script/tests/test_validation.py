from load_script.validation import validate_row

def test_validate_row_valid():
    row = {
        "trip_id": "T1",
        "agency_name": "SNCF",
        "route_name": "R1",
        "origin_stop_name": "Paris",
        "destination_stop_name": "Lyon",
        "distance_km": 400
    }
    valid, msg = validate_row(row)
    assert valid

def test_validate_row_missing_field():
    row = {
        "trip_id": "",
        "agency_name": "SNCF",
        "route_name": "R1",
        "origin_stop_name": "Paris",
        "destination_stop_name": "Lyon",
        "distance_km": 400
    }
    valid, msg = validate_row(row)
    assert not valid
    assert "trip_id" in msg

def test_validate_row_distance_out_of_range():
    row = {
        "trip_id": "T1",
        "agency_name": "SNCF",
        "route_name": "R1",
        "origin_stop_name": "Paris",
        "destination_stop_name": "Lyon",
        "distance_km": -1
    }
    valid, msg = validate_row(row)
    assert not valid
    assert "distance_km out of range" in msg