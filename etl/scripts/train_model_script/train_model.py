import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestRegressor

MODEL_PATH = "models/frequency_model_best.joblib"
DATA_PATH = "data/trips_freq.csv"

# Features après analyse EDA
NUM_FEATURES = ["distance_km", "duration_h"]
CAT_FEATURES = ["service_type", "traction"]
TARGET = "frequency_per_week"

# CORRIGÉ : NUM_FEATURES_EXTENDED promu au niveau module pour être accessible
# dans build_model() et manual_cases() sans avoir à le redéfinir partout
NUM_FEATURES_EXTENDED = ["distance_km", "duration_h", "speed_kmh", "is_night", "distance_night"]


def load_data():
    df = pd.read_csv(DATA_PATH)

    # Nettoyage
    df = df[df['distance_km'] > 0]
    df = df[df['duration_h'] > 0]
    df = df[df['frequency_per_week'] > 0]

    # Feature engineering
    df['speed_kmh'] = df['distance_km'] / df['duration_h']
    df['is_night'] = (df['service_type'] == 'NUIT').astype(int)
    df['distance_night'] = df['distance_km'] * df['is_night']

    X = df[NUM_FEATURES_EXTENDED + CAT_FEATURES]
    y = df[TARGET]

    print(f"📊 {len(df)} trajets chargés")
    print(f"   Features: {NUM_FEATURES_EXTENDED + CAT_FEATURES}")
    print(f"   Moyenne cible: {y.mean():.2f}")

    return X, y


def build_model():
    preprocessor = ColumnTransformer([
        # CORRIGÉ : utilise NUM_FEATURES_EXTENDED défini au niveau module
        ("num", StandardScaler(), NUM_FEATURES_EXTENDED),
        ("cat", OneHotEncoder(handle_unknown="ignore", drop='first'), CAT_FEATURES)
    ])
    model = make_pipeline(
        preprocessor,
        RandomForestRegressor(
            n_estimators=200,
            max_depth=None,
            min_samples_split=5,
            random_state=42,
            n_jobs=-1
        )
    )
    return model


def train_and_save():
    X, y = load_data()
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=X['is_night']
    )

    model = build_model()

    # Poids pour rééquilibrer JOUR/NUIT
    n_jour = len(X_train[X_train['is_night'] == 0])
    n_nuit = len(X_train[X_train['is_night'] == 1])
    sample_weights = X_train['is_night'].map({0: 1, 1: n_jour / n_nuit}).values

    model.fit(X_train, y_train, randomforestregressor__sample_weight=sample_weights)

    joblib.dump({"model": model, "name": "RandomForest_Optimized"}, MODEL_PATH)
    print(f"\n✓ Modèle sauvegardé → {MODEL_PATH}")
    evaluate(model, X_test, y_test)
    return model


def evaluate(model, X_test, y_test):
    y_pred = np.clip(model.predict(X_test), 1, None)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mae_pct = mae / y_test.mean() * 100
    print("\n📈 PERFORMANCES SUR TEST (20%)")
    print(f"   R²  : {r2:.4f}")
    print(f"   MAE : {mae:.2f} passages/semaine (±{mae_pct:.1f}%)")


def manual_cases(model):
    print("\n" + "=" * 55)
    print("CAS MANUELS")
    print("=" * 55)

    test_cases = [
        {
            "distance_km": 450, "duration_h": 2.5, "speed_kmh": 180, "is_night": 0, "distance_night": 0,
            "traction": "électrique", "service_type": "JOUR",
            "label": "TGV Paris→Lyon (JOUR)"
        },
        {
            "distance_km": 80, "duration_h": 1.2, "speed_kmh": 66.7, "is_night": 0, "distance_night": 0,
            "traction": "mixte", "service_type": "JOUR",
            "label": "TER court (JOUR)"
        },
        {
            "distance_km": 600, "duration_h": 4.0, "speed_kmh": 150, "is_night": 0, "distance_night": 0,
            "traction": "électrique", "service_type": "JOUR",
            "label": "International FR→DE (JOUR)"
        },
        {
            "distance_km": 450, "duration_h": 6.0, "speed_kmh": 75, "is_night": 1, "distance_night": 450,
            "traction": "électrique", "service_type": "NUIT",
            "label": "Train de nuit FR→FR"
        },
        {
            "distance_km": 30, "duration_h": 0.5, "speed_kmh": 60, "is_night": 0, "distance_night": 0,
            "traction": "électrique", "service_type": "JOUR",
            "label": "RER banlieue (JOUR)"
        },
    ]

    sample = pd.DataFrame(test_cases).drop(columns=["label"])
    preds = np.clip(model.predict(sample), 1, None)

    for tc, pred in zip(test_cases, preds):
        print(f"\n{tc['label']}")
        print(f"  → {round(pred)} passages/semaine (~{round(pred/7, 1)} par jour)")


def sanity_checks(model):
    print("\n" + "=" * 55)
    print("SANITY CHECKS")
    print("=" * 55)
    errors = []

    # Test 1: JOUR vs NUIT sur même distance
    jour = pd.DataFrame([{
        "distance_km": 450, "duration_h": 2.5, "speed_kmh": 180, "is_night": 0, "distance_night": 0,
        "traction": "électrique", "service_type": "JOUR"
    }])
    nuit = pd.DataFrame([{
        "distance_km": 450, "duration_h": 6.0, "speed_kmh": 75, "is_night": 1, "distance_night": 450,
        "traction": "électrique", "service_type": "NUIT"
    }])

    if model.predict(jour)[0] < model.predict(nuit)[0]:
        errors.append("⚠️ NUIT prédit plus fréquent que JOUR — incohérent")

    # Test 2: Court vs Long
    court = pd.DataFrame([{
        "distance_km": 30, "duration_h": 0.5, "speed_kmh": 60, "is_night": 0, "distance_night": 0,
        "traction": "électrique", "service_type": "JOUR"
    }])
    long_trip = pd.DataFrame([{
        "distance_km": 600, "duration_h": 4.0, "speed_kmh": 150, "is_night": 0, "distance_night": 0,
        "traction": "électrique", "service_type": "JOUR"
    }])

    if model.predict(court)[0] < model.predict(long_trip)[0] * 0.5:
        errors.append("⚠️ Trajet court beaucoup moins fréquent que long — à vérifier")

    # Test 3: Traction électrique vs mixte
    elec = pd.DataFrame([{
        "distance_km": 80, "duration_h": 1.2, "speed_kmh": 66.7, "is_night": 0, "distance_night": 0,
        "traction": "électrique", "service_type": "JOUR"
    }])
    mixte = pd.DataFrame([{
        "distance_km": 80, "duration_h": 1.2, "speed_kmh": 66.7, "is_night": 0, "distance_night": 0,
        "traction": "mixte", "service_type": "JOUR"
    }])

    if model.predict(elec)[0] < model.predict(mixte)[0]:
        errors.append("⚠️ Traction mixte prédite plus fréquente qu'électrique — incohérent")

    if not errors:
        print("✓ Toutes les vérifications passent")
    else:
        for e in errors:
            print(e)
    print("=" * 55)


def train():
    print("=" * 55)
    print("ENTRAÎNEMENT DU MODÈLE RANDOM FOREST")
    print("=" * 55)
    model = train_and_save()
    manual_cases(model)
    sanity_checks(model)


if __name__ == "__main__":
    train()