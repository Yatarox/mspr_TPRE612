from transform_script.gtfs_emission import estimate_traction, calculate_emissions

def test_estimate_traction():
    assert estimate_traction("101", "TGV", "SNCF", "Grande vitesse") == "électrique"
    assert estimate_traction("106", "TER", "SNCF", "Régional") == "électrique"
    assert estimate_traction("2", "AUTORAIL", "SNCF", "Régional") == "diesel"
    assert estimate_traction("2", "Train", "SNCF", "Inconnu") == "mixte"

def test_calculate_emissions():
    gco2e, kgco2e = calculate_emissions(100, "électrique", "Grande vitesse")
    assert gco2e == 3.2
    assert abs(kgco2e - 0.32) < 0.01
    gco2e, kgco2e = calculate_emissions(100, "diesel", "Régional")
    assert gco2e == 45.0
    assert abs(kgco2e - 4.5) < 0.01