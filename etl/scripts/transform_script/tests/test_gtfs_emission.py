import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_emission import estimate_traction, calculate_emissions


def test_estimate_traction():
    assert estimate_traction("101", "TGV", "SNCF", "Grande vitesse") == "électrique"

    assert estimate_traction("106", "TER", "SNCF", "Régional") == "électrique"

    assert estimate_traction("2", "AUTORAIL", "SNCF", "Régional") == "diesel"

    assert estimate_traction("2", "Train", "SNCF", "Inconnu") == "mixte"


def test_estimate_traction_additional_branches():
    # Keyword électrique dans agency_name
    assert estimate_traction("1", "Train", "EUROSTAR", "Inconnu") == "électrique"

    assert estimate_traction("1", "Train", "Compagnie X", "Intercité") == "électrique"

    assert estimate_traction("1", "Train local", "Compagnie X", "Régional") == "électrique"


def test_calculate_emissions():
    gco2e, kgco2e = calculate_emissions(100, "électrique", "Grande vitesse")
    assert gco2e == 3.2
    assert abs(kgco2e - 0.32) < 0.01

    gco2e, kgco2e = calculate_emissions(100, "diesel", "Régional")
    assert gco2e == 45.0
    assert abs(kgco2e - 4.5) < 0.01


def test_calculate_emissions_mixte_average():
    gco2e, kgco2e = calculate_emissions(100, "mixte", "Régional")
    assert gco2e == 37.45
    assert kgco2e == 3.745


def test_calculate_emissions_default_factor():
    gco2e, kgco2e = calculate_emissions(100, "diesel", "Service inconnu")
    assert gco2e == 25.0
    assert kgco2e == 2.5


def test_calculate_emissions_mixte_with_missing_keys_defaults():
    gco2e, kgco2e = calculate_emissions(100, "mixte", "Service inconnu")
    assert gco2e == 30.0
    assert kgco2e == 3.0