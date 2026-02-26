

def estimate_traction(route_type: str, route_name: str, agency_name: str, train_service: str) -> str:
    route_upper = route_name.upper()
    agency_upper = agency_name.upper()
    electric_keywords = ["TGV", "ICE", "EUROSTAR", "THALYS", "AVE", "FRECCIAROSSA", "TER", "INTERCITÉ"]
    if any(kw in route_upper or kw in agency_upper for kw in electric_keywords):
        return "électrique"
    if train_service in ["Grande vitesse", "Intercité"]:
        return "électrique"
    diesel_keywords = ["DIESEL", "AUTORAIL"]
    if any(kw in route_upper or kw in agency_upper for kw in diesel_keywords):
        return "diesel"
    if train_service == "Régional":
        return "électrique"
    return "mixte"

def calculate_emissions(distance_km: float, traction: str, train_service: str) -> tuple:
    emission_factors = {
        ("Grande vitesse", "électrique"): 3.2,
        ("Intercité", "électrique"): 8.1,
        ("Régional", "électrique"): 29.9,
        ("Inter-régional", "électrique"): 20.0,
        ("International", "électrique"): 5.0,
        ("Grande ligne", "électrique"): 6.0,
        ("Grande vitesse", "diesel"): 15.0,
        ("Intercité", "diesel"): 35.0,
        ("Régional", "diesel"): 45.0,
        ("Inter-régional", "diesel"): 40.0,
    }
    key = (train_service, traction)
    emission_gco2e_pkm = emission_factors.get(key, 25.0)
    if traction == "mixte":
        elec_key = (train_service, "électrique")
        diesel_key = (train_service, "diesel")
        emission_gco2e_pkm = (emission_factors.get(elec_key, 20.0) + emission_factors.get(diesel_key, 40.0)) / 2
    total_emission_kgco2e = round((emission_gco2e_pkm * distance_km) / 1000, 3)
    return emission_gco2e_pkm, total_emission_kgco2e

