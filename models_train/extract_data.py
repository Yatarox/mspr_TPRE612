import pandas as pd
import sqlalchemy
import os

DB_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:root@localhost/rail_dw")

def extract() -> pd.DataFrame:
    engine = sqlalchemy.create_engine(DB_URL)
    df = pd.read_sql("""
        SELECT
            f.distance_km,
            f.duration_h,
            f.frequency_per_week,
            tt.train_type,
            tr.traction,
            st.service_type,
            a.agency_name,
            oc.country_code AS origin_country,
            dc.country_code AS destination_country
        FROM fact_trip_summary f
        JOIN dim_train_type   tt ON f.train_type_sk          = tt.train_type_sk
        JOIN dim_traction     tr ON f.traction_sk            = tr.traction_sk
        JOIN dim_service_type st ON f.service_sk             = st.service_sk
        JOIN dim_agency        a ON f.agency_sk              = a.agency_sk
        JOIN dim_country      oc ON f.origin_country_sk      = oc.country_sk
        JOIN dim_country      dc ON f.destination_country_sk = dc.country_sk
        WHERE f.frequency_per_week IS NOT NULL
    """, engine)
    print(f"[extract] {len(df)} lignes chargées")
    return df

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    df = extract()
    df.to_csv("data/trips_freq.csv", index=False)
    print("[extract] Sauvegardé → data/trips_freq.csv")