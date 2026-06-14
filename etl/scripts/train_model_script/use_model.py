import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split

MODEL_PATH = "models/frequency_model_best.joblib"
DATA_PATH = "data/trips_freq.csv"

NUM_FEATURES = ["distance_km", "duration_h", "speed_kmh", "is_night", "distance_night"]
CAT_FEATURES = ["service_type", "traction"]
TARGET = "frequency_per_week"

def load_model(path=MODEL_PATH):
    artifact = joblib.load(path)
    return artifact["model"], artifact["name"]

def load_data(path=DATA_PATH):
    df = pd.read_csv(path)
    
    df = df[df['distance_km'] > 0]
    df = df[df['duration_h'] > 0]
    df = df[df['frequency_per_week'] > 0]
    
    df['speed_kmh'] = df['distance_km'] / df['duration_h']
    df['is_night'] = (df['service_type'] == 'NUIT').astype(int)
    df['distance_night'] = df['distance_km'] * df['is_night']
    
    X = df[NUM_FEATURES + CAT_FEATURES]
    y = df[TARGET]
    
    print(f"📊 {len(df)} trajets chargés")
    print(f"   Features: {NUM_FEATURES + CAT_FEATURES}")
    print(f"   Moyenne cible: {y.mean():.2f}")
    
    return X, y

def evaluate(model, X, y):
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    y_pred = np.clip(model.predict(X_test), 1, None)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mae_pct = mae / y_test.mean() * 100
    return {
        "r2": r2,
        "mae": mae,
        "mae_pct": mae_pct,
        "y_test": y_test,
        "y_pred": y_pred
    }

def summary_model():
    model, name = load_model()
    X, y = load_data()
    metrics = evaluate(model, X, y)
    print(f"\nModèle : {name}")
    print("=" * 55)
    print(f"R²  : {metrics['r2']:.4f}")
    print(f"MAE : {metrics['mae']:.2f} passages/semaine (±{metrics['mae_pct']:.1f}%)")
    return metrics

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
    
    sample = pd.DataFrame(test_cases)
    preds = np.clip(model.predict(sample), 1, None)
    
    for tc, pred in zip(test_cases, preds):
        print(f"\n{tc['label']}")
        print(f"  → {round(pred)} passages/semaine (~{round(pred/7, 1)} par jour)")

def use_model():
    metrics = summary_model()
    model, _ = load_model()
    manual_cases(model)
    return metrics

if __name__ == "__main__":
    use_model()