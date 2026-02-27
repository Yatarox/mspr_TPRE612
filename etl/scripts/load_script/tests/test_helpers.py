from load_script.helpers import sanitize_country_for_staging

def test_sanitize_country_for_staging_basic():
    assert sanitize_country_for_staging("FR", 5, "origin_country") == "FR"
    assert sanitize_country_for_staging("fr", 5, "origin_country") == "FR"
    assert sanitize_country_for_staging("France", 5, "origin_country") == "FRANC"
    assert sanitize_country_for_staging("2022-01-01", 5, "origin_country") is None
    assert sanitize_country_for_staging("UNK", 5, "origin_country") is None
    assert sanitize_country_for_staging(None, 5, "origin_country") is None
    assert sanitize_country_for_staging("", 5, "origin_country") is None

def test_sanitize_country_for_staging_special_cases():
    # Test valeurs spéciales
    assert sanitize_country_for_staging("N/A", 5, "origin_country") is None
    assert sanitize_country_for_staging("NULL", 5, "origin_country") is None
    assert sanitize_country_for_staging("   ", 5, "origin_country") is None
    # Test caractères spéciaux
    assert sanitize_country_for_staging("F-R!", 5, "origin_country") == "FR"
    # Test longueur max
    assert sanitize_country_for_staging("ABCDEFGHIJK", 5, "origin_country") == "ABCDE"
    # Test float NaN
    assert sanitize_country_for_staging(float('nan'), 5, "origin_country") is None