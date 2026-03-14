import logging

import pandas as pd
from typing import Dict, Optional
import numpy as np
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_stop_country_map(stops_df: pd.DataFrame) -> Dict[str, Optional[str]]:
    if stops_df.empty or "stop_lat" not in stops_df.columns or "stop_lon" not in stops_df.columns:
        logger.warning("No lat/lon in stops.txt, returning empty country map")
        return {}
    country_boxes = {
        "FR": (41.0, 51.5, -5.5, 10.0),
        "DE": (47.0, 55.5, 5.5, 15.5),
        "IT": (36.0, 47.5, 6.5, 19.0),
        "ES": (35.5, 44.0, -10.0, 5.0),
        "CH": (45.5, 48.0, 5.5, 11.0),
        "BE": (49.5, 51.5, 2.5, 6.5),
        "NL": (50.5, 54.0, 3.0, 7.5),
        "AT": (46.0, 49.5, 9.0, 17.5),
        "LU": (49.4, 50.2, 5.7, 6.6),
        "GB": (49.5, 61.0, -8.5, 2.0),
        "IE": (51.0, 55.5, -11.0, -5.5),
        "PT": (36.5, 42.5, -10.0, -6.0),
        "PL": (49.0, 55.0, 14.0, 24.5),
        "CZ": (48.5, 51.0, 12.0, 19.0),
        "SK": (47.5, 49.5, 16.5, 22.5),
        "HU": (45.5, 48.5, 16.0, 23.0),
        "RO": (43.5, 48.5, 20.0, 30.0),
        "BG": (41.0, 44.5, 22.0, 29.0),
        "GR": (34.5, 42.0, 19.0, 29.0),
        "HR": (42.0, 46.5, 13.0, 20.0),
        "SI": (45.0, 47.0, 13.0, 17.0),
        "SE": (55.0, 69.5, 10.5, 24.5),
        "NO": (57.5, 71.5, 4.0, 31.5),
        "DK": (54.5, 58.0, 8.0, 15.5),
        "FI": (59.5, 70.5, 19.0, 32.0),
    }
    stop_country_map = {}
    stops_clean = stops_df.copy()
    stops_clean["stop_lat"] = pd.to_numeric(stops_clean["stop_lat"], errors="coerce")
    stops_clean["stop_lon"] = pd.to_numeric(stops_clean["stop_lon"], errors="coerce")
    stops_clean = stops_clean.dropna(subset=["stop_lat", "stop_lon", "stop_id"])
    logger.info(f"🌍 Building country map for {len(stops_clean)} stops with valid coordinates...")
    for _, stop in stops_clean.iterrows():
        stop_id = str(stop["stop_id"])
        lat = float(stop["stop_lat"])
        lon = float(stop["stop_lon"])
        matches = []
        for country, (lat_min, lat_max, lon_min, lon_max) in country_boxes.items():
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                matches.append(country)
        if len(matches) == 1:
            stop_country_map[stop_id] = matches[0]
        elif len(matches) > 1:
            stop_country_map[stop_id] = matches[0]
            logger.debug(f"Stop {stop_id} at ({lat:.4f}, {lon:.4f}) in multiple countries {matches}, using {matches[0]}")
        else:
            stop_country_map[stop_id] = None
            logger.debug(f"Stop {stop_id} at ({lat:.4f}, {lon:.4f}) outside known countries")
    found_count = sum(1 for v in stop_country_map.values() if v is not None)
    logger.info(f"✓ Country map built: {found_count}/{len(stops_clean)} stops mapped ({found_count/len(stops_clean)*100:.1f}%)")
    country_counts = {}
    for country in stop_country_map.values():
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1
    logger.info(f"📊 Stops per country: {dict(sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:10])}")
    return stop_country_map

def extract_country_from_stop_name(stop_name: str) -> Optional[str]:
    stop_upper = stop_name.upper()
    country_patterns = {
        "FR": ["PARIS", "LYON", "MARSEILLE", "LILLE", "STRASBOURG", "BORDEAUX", "NANTES", "TOULOUSE", "NICE", "MONTPELLIER"],
        "DE": ["BERLIN", "MÜNCHEN", "MUNICH", "FRANKFURT", "HAMBURG", "KÖLN", "COLOGNE", "STUTTGART"],
        "IT": ["ROMA", "ROME", "MILANO", "MILAN", "VENEZIA", "VENICE", "FIRENZE", "FLORENCE", "NAPOLI"],
        "ES": ["MADRID", "BARCELONA", "VALENCIA", "SEVILLA", "SEVILLE", "ZARAGOZA"],
        "CH": ["ZÜRICH", "ZURICH", "GENÈVE", "GENEVA", "BERN", "BERNE", "BASEL"],
        "BE": ["BRUXELLES", "BRUSSELS", "BRUSSEL", "ANTWERPEN", "ANVERS", "GENT"],
        "NL": ["AMSTERDAM", "ROTTERDAM", "UTRECHT", "EINDHOVEN", "TILBURG"],
        "AT": ["WIEN", "VIENNA", "GRAZ", "LINZ", "SALZBURG"],
        "GB": ["LONDON", "MANCHESTER", "BIRMINGHAM", "LEEDS", "GLASGOW"],
        "CZ": ["PRAHA", "PRAGUE", "BRNO", "OSTRAVA"],
        "PL": ["WARSZAWA", "WARSAW", "KRAKÓW", "CRACOW", "ŁÓDŹ"],
        "LU": ["LUXEMBOURG"],
        "PT": ["LISBOA", "LISBON", "PORTO"],
    }
    for country, cities in country_patterns.items():
        if any(city in stop_upper for city in cities):
            return country
    return None


# ============================================================
# Distance
# ============================================================

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def compute_distances(stop_times: pd.DataFrame, stops: pd.DataFrame) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)
    st = stop_times[["trip_id", "stop_id", "stop_sequence"]].copy()

    if "shape_dist_traveled" in stop_times.columns:
        st["shape_dist_traveled"] = pd.to_numeric(stop_times["shape_dist_traveled"], errors="coerce")
        st = st.sort_values(["trip_id", "stop_sequence"])
        if st["shape_dist_traveled"].notna().any():
            g = st.groupby("trip_id")["shape_dist_traveled"]
            raw = (g.max() - g.min())
            raw = raw.where(raw <= 1000, raw / 1000.0)
            return raw.fillna(0).round(3)

    st = st.sort_values(["trip_id", "stop_sequence"])
    if "stop_lat" not in stops.columns or "stop_lon" not in stops.columns:
        logger.warning("stop_lat ou stop_lon not found in stops.txt, returning 0 distances")
        return pd.Series(dtype=float)

    stops_idx = stops[["stop_id", "stop_lat", "stop_lon"]].copy()
    stops_idx["stop_lat"] = pd.to_numeric(stops_idx["stop_lat"], errors="coerce")
    stops_idx["stop_lon"] = pd.to_numeric(stops_idx["stop_lon"], errors="coerce")
    stops_idx = stops_idx.set_index("stop_id")

    st = st.join(stops_idx, on="stop_id", how="left")
    st.loc[:, "lat_prev"] = st.groupby("trip_id")["stop_lat"].shift(1)
    st.loc[:, "lon_prev"] = st.groupby("trip_id")["stop_lon"].shift(1)

    seg = st.dropna(subset=["lat_prev", "lon_prev", "stop_lat", "stop_lon"])
    if seg.empty:
        logger.warning("No valid segments for distance calculation")
        return pd.Series(dtype=float)

    seg = seg.copy()
    seg["seg_km"] = haversine_km(
        seg["lat_prev"].astype(float),
        seg["lon_prev"].astype(float),
        seg["stop_lat"].astype(float),
        seg["stop_lon"].astype(float),
    )
    result = seg.groupby("trip_id")["seg_km"].sum().round(3)
    return result.fillna(0)
