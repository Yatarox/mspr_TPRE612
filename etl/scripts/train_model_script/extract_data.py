import pandas as pd
import sqlalchemy
import os

DB_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:root@mysql/rail_dw")

def extract_all_trips() -> pd.DataFrame:
    """Extrait tous les trajets depuis la base de données"""
    engine = sqlalchemy.create_engine(DB_URL)
    
    df = pd.read_sql("""
        SELECT
            t.trip_id,
            f.distance_km,
            f.duration_h,
            f.frequency_per_week,
            f.emission_gco2e_pkm,
            f.total_emission_kgco2e,
            tt.train_type,
            tr.traction,
            st.service_type,
            a.agency_name,
            oc.country_code AS origin_country,
            dc.country_code AS destination_country
        FROM fact_trip_summary f
        JOIN dim_trip         t  ON f.trip_sk               = t.trip_sk
        JOIN dim_train_type   tt ON f.train_type_sk         = tt.train_type_sk
        JOIN dim_traction     tr ON f.traction_sk           = tr.traction_sk
        JOIN dim_service_type st ON f.service_sk            = st.service_sk
        JOIN dim_agency        a ON f.agency_sk             = a.agency_sk
        JOIN dim_country      oc ON f.origin_country_sk     = oc.country_sk
        JOIN dim_country      dc ON f.destination_country_sk = dc.country_sk
        WHERE f.frequency_per_week IS NOT NULL
          AND f.distance_km > 0
          AND f.duration_h > 0
    """, engine)
    
    print(f"[extract] {len(df)} lignes chargées depuis la BDD")
    return df

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prépare les données pour l'entraînement"""
    
    print(f"\n📊 Avant nettoyage : {len(df)} lignes")
    
    # Supprimer les doublons
    df = df.drop_duplicates(subset=['trip_id'], keep='first')
    print(f"   Après dédoublonnage : {len(df)} lignes")

    # Filtre Python (miroir de la clause WHERE SQL)
    df = df[df['distance_km'] > 0]
    df = df[df['duration_h'] > 0]
    print(f"   Après filtre distance/duration > 0 : {len(df)} lignes")

    # Feature engineering
    df['speed_kmh'] = df['distance_km'] / df['duration_h']
    df['is_night'] = (df['service_type'] == 'NUIT').astype(int)
    df['distance_night'] = df['distance_km'] * df['is_night']
    df['is_international'] = (df['origin_country'] != df['destination_country']).astype(int)
    
    # Nettoyer les outliers sur la cible
    Q99 = df['frequency_per_week'].quantile(0.99)
    df = df[df['frequency_per_week'] <= Q99]
    print(f"   Après suppression outliers (>99e percentile) : {len(df)} lignes")
    
    # Vérifier les valeurs manquantes
    print("\n📋 Valeurs manquantes :")
    print(df.isnull().sum()[df.isnull().sum() > 0])
    
    # Supprimer les lignes avec des valeurs critiques manquantes
    critical_cols = ['distance_km', 'duration_h', 'frequency_per_week', 'service_type']
    df = df.dropna(subset=critical_cols)
    print(f"   Après suppression NaN critiques : {len(df)} lignes")
    
    return df

def analyze_data(df: pd.DataFrame):
    """Analyse rapide des données"""
    print("\n" + "=" * 60)
    print("ANALYSE DES DONNÉES EXTRAITES")
    print("=" * 60)
    
    print("\n📊 Statistiques générales :")
    print(f"   Total trajets : {len(df)}")
    print(f"   Train types : {df['train_type'].unique().tolist()}")
    print(f"   Service types : {df['service_type'].value_counts().to_dict()}")
    print(f"   Tractions : {df['traction'].value_counts().to_dict()}")
    print(f"   Pays : {df['origin_country'].nunique()} origines, {df['destination_country'].nunique()} destinations")
    
    print("\n📈 Variable cible (frequency_per_week) :")
    print(f"   Moyenne : {df['frequency_per_week'].mean():.2f}")
    print(f"   Médiane : {df['frequency_per_week'].median():.2f}")
    print(f"   Min/Max : {df['frequency_per_week'].min()} / {df['frequency_per_week'].max()}")
    
    print("\n📉 Corrélations avec la cible :")
    corr = df[['distance_km', 'duration_h', 'speed_kmh', 'is_night', 'frequency_per_week']].corr()
    print(corr['frequency_per_week'].sort_values(ascending=False))

def save_data(df: pd.DataFrame, path: str = "data/trips_freq.csv"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"\n✅ Données sauvegardées : {path} ({len(df)} lignes)")

def extract() -> pd.DataFrame:
    """Pipeline complet d'extraction et préparation des données"""
    df_raw = extract_all_trips()
    df_clean = prepare_data(df_raw)
    analyze_data(df_clean)
    save_data(df_clean)
    return df_clean

if __name__ == "__main__":
    df_raw = extract_all_trips()
    df_clean = prepare_data(df_raw)
    analyze_data(df_clean)
    save_data(df_clean)
    print("\n✨ Prêt pour l'entraînement !")