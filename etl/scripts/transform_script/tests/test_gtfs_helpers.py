from transform_script.gtfs_helpers import is_valid_numeric, get_transport_type

def test_is_valid_numeric():
    assert is_valid_numeric("123")
    assert is_valid_numeric("123.45")
    assert not is_valid_numeric("abc")
    assert not is_valid_numeric("12/34")
    assert not is_valid_numeric("12-34-56")
    assert not is_valid_numeric("")

def test_get_transport_type():
    assert get_transport_type("2") == "Rail"
    assert get_transport_type("101") == "High Speed Rail"
    assert get_transport_type("9999") == "Type 9999"