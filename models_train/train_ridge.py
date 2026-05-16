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

artifact = joblib.load(MODEL_PATH)
model    = artifact["model"]
name     = artifact["name"]

df = pd.read_csv(DATA_PATH).dropna(subset=NUM_FEATURES + CAT_FEATURES + [TARGET])
X  = df[NUM_FEATURES + CAT_FEATURES]
y  = df[TARGET]

_, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
y_pred = np.clip(model.predict(X_test), 1, None)

print(f"Modèle : {name}")
print("=" * 55)
print(f"R²  : {r2_score(y_test, y_pred):.4f}")
mae = mean_absolute_error(y_test, y_pred)
print(f"MAE : {mae:.2f} passages/semaine (±{mae/y_test.mean()*100:.1f}%)")

print("\nCAS MANUELS")
print("=" * 55)

test_cases = [
    {
        "distance_km": 450, "duration_h": 2.5,
        "train_type": "Grande vitesse", "traction": "Électrique",
        "service_type": "JOUR", "origin_country": "FR", "destination_country": "FR",
        "label": "TGV Paris→Lyon (JOUR)"
    },
    {
        "distance_km": 80, "duration_h": 1.2,
        "train_type": "Régional", "traction": "Diesel",
        "service_type": "JOUR", "origin_country": "FR", "destination_country": "FR",
        "label": "TER court (JOUR)"
    },
    {
        "distance_km": 600, "duration_h": 4.0,
        "train_type": "International", "traction": "Électrique",
        "service_type": "JOUR", "origin_country": "FR", "destination_country": "DE",
        "label": "International FR→DE (JOUR)"
    },
    {
        "distance_km": 450, "duration_h": 6.0,
        "train_type": "Grande vitesse", "traction": "Électrique",
        "service_type": "NUIT", "origin_country": "FR", "destination_country": "FR",
        "label": "TGV de nuit FR→FR"
    },
    {
        "distance_km": 30, "duration_h": 0.5,
        "train_type": "Régional", "traction": "Électrique",
        "service_type": "JOUR", "origin_country": "FR", "destination_country": "FR",
        "label": "RER banlieue (JOUR)"
    },
]

labels  = [c.pop("label") for c in test_cases]
sample  = pd.DataFrame(test_cases)
preds   = np.clip(model.predict(sample), 1, None)

for label, pred in zip(labels, preds):
    print(f"\n{label}")
    print(f"  Fréquence prédite : {round(pred)} passages/semaine")
    print(f"  Soit ~{round(pred/7, 1)} passages/jour")

# ── Sanity checks ──────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("SANITY CHECKS")
print("=" * 55)

errors = []

# JOUR doit prédire plus que NUIT à distance égale
jour = pd.DataFrame([test_cases[0] | {"service_type": "JOUR"}])
nuit = pd.DataFrame([test_cases[0] | {"service_type": "NUIT"}])
if model.predict(jour)[0] < model.predict(nuit)[0]:
    errors.append("⚠ NUIT prédit plus fréquent que JOUR — incohérent")

# Court trajet doit pas prédire moins qu'un long
court = pd.DataFrame([test_cases[4]])
long_ = pd.DataFrame([test_cases[2]])
if model.predict(court)[0] < model.predict(long_)[0] * 0.5:
    errors.append("⚠ Trajet court beaucoup moins fréquent que long — à vérifier")

if not errors:
    print("✓ Toutes les vérifications passent")
else:
    for e in errors:
        print(e)

print("=" * 55)