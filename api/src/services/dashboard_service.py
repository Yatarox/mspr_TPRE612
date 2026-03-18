from models.database import execute_query
from datetime import datetime


async def get_overview():
    query = """
        SELECT
            COUNT(DISTINCT f.trip_sk) as total_trips,
            COUNT(DISTINCT f.route_sk) as total_routes,
            COUNT(DISTINCT f.agency_sk) as total_agencies,
            ROUND(SUM(f.distance_km), 2) as total_distance_km,
            ROUND(SUM(f.total_emission_kgco2e), 2) as total_emissions_kg,
            ROUND(AVG(f.distance_km), 2) as avg_distance_km,
            ROUND(AVG(f.duration_h), 2) as avg_duration_h,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km
        FROM fact_trip_summary f
    """
    result = await execute_query(query)
    return result[0] if result else {}


async def get_stats_by_country():
    query = """
        SELECT
            COALESCE(c.country_code, 'UNKNOWN') as country,
            COALESCE(c.country_name, 'Unknown') as country_name,
            COUNT(DISTINCT f.trip_sk) as trip_count,
            ROUND(SUM(f.distance_km), 2) as total_distance,
            ROUND(SUM(f.total_emission_kgco2e), 2) as total_emissions,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km
        FROM fact_trip_summary f
        LEFT JOIN dim_country c ON c.country_sk = f.origin_country_sk
        GROUP BY c.country_code, c.country_name
        ORDER BY trip_count DESC
        LIMIT 20
    """
    return await execute_query(query)


async def get_stats_by_train_type():
    query = """
        SELECT
            COALESCE(tt.train_type, 'Unknown') as train_type,
            COUNT(DISTINCT f.trip_sk) as trip_count,
            ROUND(SUM(f.distance_km), 2) as total_distance,
            ROUND(AVG(f.distance_km), 2) as avg_distance,
            ROUND(AVG(f.duration_h), 2) as avg_duration,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km
        FROM fact_trip_summary f
        LEFT JOIN dim_train_type tt ON tt.train_type_sk = f.train_type_sk
        GROUP BY tt.train_type
        ORDER BY trip_count DESC
    """
    return await execute_query(query)


async def get_stats_by_traction():
    query = """
        SELECT
            COALESCE(tr.traction, 'Unknown') as traction,
            COUNT(DISTINCT f.trip_sk) as trip_count,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km,
            ROUND(SUM(f.total_emission_kgco2e), 2) as total_emissions
        FROM fact_trip_summary f
        LEFT JOIN dim_traction tr ON tr.traction_sk = f.traction_sk
        GROUP BY tr.traction
        ORDER BY trip_count DESC
    """
    return await execute_query(query)


async def get_stats_by_agency(limit: int):
    query = """
        SELECT
            a.agency_name,
            COUNT(DISTINCT f.trip_sk) as trip_count,
            ROUND(SUM(f.distance_km), 2) as total_distance,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km
        FROM fact_trip_summary f
        JOIN dim_agency a ON a.agency_sk = f.agency_sk
        GROUP BY a.agency_name
        ORDER BY trip_count DESC
        LIMIT %s
    """
    return await execute_query(query, (limit,))


async def get_emissions_by_route(limit: int):
    query = """
        SELECT
            r.route_name,
            a.agency_name,
            COUNT(DISTINCT f.trip_sk) as trip_count,
            ROUND(SUM(f.total_emission_kgco2e), 2) as total_emissions,
            ROUND(AVG(f.emission_gco2e_pkm), 2) as avg_emission_per_km,
            ROUND(AVG(f.distance_km), 2) as avg_distance
        FROM fact_trip_summary f
        JOIN dim_route r ON r.route_sk = f.route_sk
        JOIN dim_agency a ON a.agency_sk = f.agency_sk
        GROUP BY r.route_name, a.agency_name
        ORDER BY total_emissions DESC
        LIMIT %s
    """
    return await execute_query(query, (limit,))


async def search_trips(
    origin,
    destination,
    train_type,
    min_distance,
    max_distance,
    limit
):
    where_clauses = []
    params = []

    if origin:
        where_clauses.append("lo.stop_name LIKE %s")
        params.append(f"%{origin}%")

    if destination:
        where_clauses.append("ld.stop_name LIKE %s")
        params.append(f"%{destination}%")

    if train_type:
        where_clauses.append("tt.train_type = %s")
        params.append(train_type)

    if min_distance is not None:
        where_clauses.append("f.distance_km >= %s")
        params.append(min_distance)

    if max_distance is not None:
        where_clauses.append("f.distance_km <= %s")
        params.append(max_distance)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = (
        "WITH ranked AS ("
        "  SELECT "
        "    f.fact_sk, f.trip_sk, f.service_sk, f.dataset_sk, "
        "    f.distance_km, f.duration_h, f.total_emission_kgco2e, f.frequency_per_week, f.last_loaded_at, "
        "    t.trip_id, "
        "    r.route_name, a.agency_name, "
        "    lo.stop_name AS origin, ld.stop_name AS destination, "
        "    co.country_code AS origin_country, cd.country_code AS destination_country, "
        "    tdep.time_value AS departure_time, tarr.time_value AS arrival_time, "
        "    tt.train_type, tr.traction, "
        "    ds.service_type AS service_type, "
        "    CASE "
        "      WHEN UPPER(COALESCE(ds.service_type,'')) = 'JOUR' THEN 'Jour' "
        "      WHEN UPPER(COALESCE(ds.service_type,'')) = 'NUIT' THEN 'Nuit' "
        "      ELSE 'Autre / Non renseigné' "
        "    END AS service_label, "
        "    ROW_NUMBER() OVER ("
        "      PARTITION BY t.trip_id "
        "      ORDER BY f.last_loaded_at DESC, f.fact_sk DESC"
        "    ) AS rn "
        "  FROM fact_trip_summary f "
        "  JOIN dim_trip t ON t.trip_sk = f.trip_sk "
        "  JOIN dim_route r ON r.route_sk = f.route_sk "
        "  JOIN dim_agency a ON a.agency_sk = f.agency_sk "
        "  LEFT JOIN dim_location lo ON lo.location_sk = f.origin_location_sk "
        "  LEFT JOIN dim_location ld ON ld.location_sk = f.destination_location_sk "
        "  LEFT JOIN dim_country co ON co.country_sk = f.origin_country_sk "
        "  LEFT JOIN dim_country cd ON cd.country_sk = f.destination_country_sk "
        "  LEFT JOIN dim_time tdep ON tdep.time_sk = f.departure_time_sk "
        "  LEFT JOIN dim_time tarr ON tarr.time_sk = f.arrival_time_sk "
        "  LEFT JOIN dim_train_type tt ON tt.train_type_sk = f.train_type_sk "
        "  LEFT JOIN dim_traction tr ON tr.traction_sk = f.traction_sk "
        "  LEFT JOIN dim_service_type ds ON ds.service_sk = f.service_sk "
        f"  WHERE {where_sql} "
        ") "
        "SELECT * FROM ranked WHERE rn = 1 "
        "ORDER BY distance_km DESC "
        "LIMIT %s"
    )

    params.append(limit if limit is not None else 50)
    return await execute_query(query, tuple(params))




async def get_health():
    try:
        result = await execute_query(
            "SELECT COUNT(*) as count FROM fact_trip_summary"
        )
        return {
            "status": "healthy",
            "database": "connected",
            "total_trips": result[0]["count"] if result else 0,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
    

# ...existing code...

async def get_stats_by_service_type():
    query = """
        SELECT
            CASE
                WHEN UPPER(COALESCE(ds.service_type, '')) = 'JOUR' THEN 'Jour'
                WHEN UPPER(COALESCE(ds.service_type, '')) = 'NUIT' THEN 'Nuit'
                ELSE 'Autre / Non renseigné'
            END AS service_type,
            COUNT(DISTINCT f.trip_sk) AS trip_count
        FROM fact_trip_summary f
        LEFT JOIN dim_service_type ds ON ds.service_sk = f.service_sk
        GROUP BY
            CASE
                WHEN UPPER(COALESCE(ds.service_type, '')) = 'JOUR' THEN 'Jour'
                WHEN UPPER(COALESCE(ds.service_type, '')) = 'NUIT' THEN 'Nuit'
                ELSE 'Autre / Non renseigné'
            END
        ORDER BY trip_count DESC
    """
    return await execute_query(query)

# ...existing code...

