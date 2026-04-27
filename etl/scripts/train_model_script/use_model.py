import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split

MODEL_PATH = "models/frequency_model.joblib"
DATA_PATH  = "data/trips_freq.csv"

NUM_FEATURES = ["distance_km", "duration_h"]
CAT_FEATURES = ["train_type", "traction", "service_type", "origin_country", "destination_country"]
TARGET       = "frequency_per_week"

def load_model(path=MODEL_PATH):
    artifact = joblib.load(path)
    return artifact["model"], artifact["name"]

def load_data(path=DATA_PATH):
    df = pd.read_csv(path).dropna(subset=NUM_FEATURES + CAT_FEATURES + [TARGET])
    X = df[NUM_FEATURES + CAT_FEATURES]
    y = df[TARGET]
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
    print(f"Modèle : {name}")
    print("=" * 55)
    print(f"R²  : {metrics['r2']:.4f}")
    print(f"MAE : {metrics['mae']:.2f} passages/semaine (±{metrics['mae_pct']:.1f}%)")
    return metrics

def manual_cases(model):
    print("\nCAS MANUELS")
    print("=" * 55)
    test_cases = [
        {"distance_km": 450, "duration_h": 2.5, "train_type": "Grande vitesse", "traction": "Électrique", "service_type": "Haute vitesse", "origin_country": "FR", "destination_country": "FR"},
        {"distance_km": 80,  "duration_h": 1.2, "train_type": "Régional",       "traction": "Diesel",     "service_type": "Régional",      "origin_country": "FR", "destination_country": "FR"},
        {"distance_km": 600, "duration_h": 4.0, "train_type": "International",   "traction": "Électrique", "service_type": "International", "origin_country": "FR", "destination_country": "DE"},
        {"distance_km": 200, "duration_h": 2.0, "train_type": "Intercité",       "traction": "Électrique", "service_type": "Intercité",     "origin_country": "FR", "destination_country": "FR"},
    ]
    sample = pd.DataFrame(test_cases)
    preds  = np.clip(model.predict(sample), 1, None)
    for case, pred in zip(test_cases, preds):
        print(f"\n{case['train_type']} {case['origin_country']}→{case['destination_country']} ({case['distance_km']}km)")
        print(f"  Fréquence prédite : {round(pred)} passages/semaine")
        print(f"  Soit ~{round(pred/7, 1)} passages/jour")

def use_model():
    metrics = summary_model()
    model, _ = load_model()
    manual_cases(model)
    return metrics

if __name__ == "__main__":
    use_model()