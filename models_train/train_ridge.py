import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge

DATA_PATH  = "data/trips_freq.csv"
MODEL_PATH = "models/frequency_model.joblib"

NUM_FEATURES = ["distance_km", "duration_h"]
CAT_FEATURES = ["train_type", "traction", "service_type", "origin_country", "destination_country"]
TARGET       = "frequency_per_week"

# ── Chargement des données ────────────────────────────────────────────────────

df = pd.read_csv(DATA_PATH).dropna(subset=NUM_FEATURES + CAT_FEATURES + [TARGET])
X  = df[NUM_FEATURES + CAT_FEATURES]
y  = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ── Préprocesseur commun ──────────────────────────────────────────────────────

def build_pipeline(estimator):
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), NUM_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CAT_FEATURES),
    ])
    return make_pipeline(preprocessor, estimator)

# ── Modèles à comparer ────────────────────────────────────────────────────────

models = {
    "Random Forest":       RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "Ridge":               Ridge(alpha=1.0),
    "Gradient Boosting":   GradientBoostingRegressor(n_estimators=100, random_state=42),
}

# ── Entraînement et évaluation ────────────────────────────────────────────────

print("=" * 65)
print("COMPARAISON DES MODÈLES — Prédiction fréquence ferroviaire")
print("=" * 65)
print(f"{'Modèle':<25} {'R²':>8} {'MAE':>10} {'MAE%':>8} {'CV R² (5-fold)':>16}")
print("-" * 65)

results = {}

for name, estimator in models.items():
    pipeline = build_pipeline(estimator)
    pipeline.fit(X_train, y_train)

    y_pred = np.clip(pipeline.predict(X_test), 1, None)
    r2  = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mae_pct = mae / y_test.mean() * 100

    cv_scores = cross_val_score(pipeline, X, y, cv=5, scoring="r2", n_jobs=-1)
    cv_mean   = cv_scores.mean()

    results[name] = {
        "pipeline": pipeline,
        "r2": r2,
        "mae": mae,
        "mae_pct": mae_pct,
        "cv_r2": cv_mean,
    }

    print(f"{name:<25} {r2:>8.4f} {mae:>10.2f} {mae_pct:>7.1f}% {cv_mean:>15.4f}")

print("=" * 65)

# ── Meilleur modèle ───────────────────────────────────────────────────────────

best_name = max(results, key=lambda k: results[k]["r2"])
best      = results[best_name]

print(f"\n🏆 Meilleur modèle : {best_name}")
print(f"   R²  : {best['r2']:.4f}")
print(f"   MAE : {best['mae']:.2f} passages/semaine (±{best['mae_pct']:.1f}%)")
print(f"   CV R² (5-fold) : {best['cv_r2']:.4f}")

# ── Cas manuels sur le meilleur modèle ───────────────────────────────────────

print("\nCAS MANUELS")
print("=" * 65)

test_cases = [
    {"distance_km": 450, "duration_h": 2.5, "train_type": "Grande vitesse",
     "traction": "Électrique", "service_type": "JOUR", "origin_country": "FR",
     "destination_country": "FR", "label": "TGV Paris→Lyon (JOUR)"},
    {"distance_km": 80,  "duration_h": 1.2, "train_type": "Régional",
     "traction": "Diesel",     "service_type": "JOUR", "origin_country": "FR",
     "destination_country": "FR", "label": "TER court (JOUR)"},
    {"distance_km": 600, "duration_h": 4.0, "train_type": "International",
     "traction": "Électrique", "service_type": "JOUR", "origin_country": "FR",
     "destination_country": "DE", "label": "International FR→DE (JOUR)"},
    {"distance_km": 450, "duration_h": 6.0, "train_type": "Grande vitesse",
     "traction": "Électrique", "service_type": "NUIT", "origin_country": "FR",
     "destination_country": "FR", "label": "TGV de nuit FR→FR"},
    {"distance_km": 30,  "duration_h": 0.5, "train_type": "Régional",
     "traction": "Électrique", "service_type": "JOUR", "origin_country": "FR",
     "destination_country": "FR", "label": "RER banlieue (JOUR)"},
]

labels = [c.pop("label") for c in test_cases]
sample = pd.DataFrame(test_cases)

print(f"\n{'Cas':<30}", end="")
for name in models:
    print(f"  {name[:18]:<18}", end="")
print()
print("-" * 95)

all_preds = {
    name: np.clip(results[name]["pipeline"].predict(sample), 1, None)
    for name in models
}

for i, label in enumerate(labels):
    print(f"{label:<30}", end="")
    for name in models:
        pred = all_preds[name][i]
        print(f"  {round(pred):>5} / sem        ", end="")
    print()

# ── Sanity checks ─────────────────────────────────────────────────────────────

print("\n" + "=" * 65)
print("SANITY CHECKS")
print("=" * 65)

best_pipeline = best["pipeline"]
errors = []

jour = pd.DataFrame([{**test_cases[0], "service_type": "JOUR"}])
nuit = pd.DataFrame([{**test_cases[0], "service_type": "NUIT"}])
if best_pipeline.predict(jour)[0] < best_pipeline.predict(nuit)[0]:
    errors.append("⚠ NUIT prédit plus fréquent que JOUR — incohérent")

court = pd.DataFrame([test_cases[4]])
long_ = pd.DataFrame([test_cases[2]])
if best_pipeline.predict(court)[0] < best_pipeline.predict(long_)[0] * 0.5:
    errors.append("⚠ Trajet court beaucoup moins fréquent que long — à vérifier")

if not errors:
    print("✓ Toutes les vérifications passent")
else:
    for e in errors:
        print(e)

# ── Sauvegarde du meilleur modèle ─────────────────────────────────────────────

joblib.dump({"model": best["pipeline"], "name": best_name}, MODEL_PATH)
print(f"\n✓ Modèle '{best_name}' sauvegardé → {MODEL_PATH}")
print("=" * 65)