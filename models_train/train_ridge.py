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
import os
DATA_PATH = "data/test2.csv"
MODEL_PATH = "models/frequency_model.joblib"

os.makedirs("models", exist_ok=True)
# Chargement
df = pd.read_csv(DATA_PATH)

# Nettoyage final
df = df[df['distance_km'] > 0]
df = df[df['duration_h'] > 0]
df = df[df['frequency_per_week'] > 0]

# Features (après EDA)
NUM_FEATURES = ["distance_km", "duration_h"]
CAT_FEATURES = ["service_type", "traction"]
TARGET = "frequency_per_week"

X = df[NUM_FEATURES + CAT_FEATURES]
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=X['service_type']
)

# Calcul des poids pour rééquilibrer JOUR/NUIT
# Les trains de nuit sont sous-représentés (149 vs 2932)
n_jour = len(X_train[X_train['service_type'] == 'JOUR'])
n_nuit = len(X_train[X_train['service_type'] == 'NUIT'])
sample_weights = X_train['service_type'].map({'JOUR': 1, 'NUIT': n_jour / n_nuit}).values

# Préprocesseur
def build_pipeline(estimator):
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), NUM_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore", drop='first'), CAT_FEATURES),
    ])
    return make_pipeline(preprocessor, estimator)

models = {
    "Ridge": Ridge(alpha=1.0),
    "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
}

print("=" * 65)
print("COMPARAISON DES MODÈLES — Prédiction fréquence ferroviaire")
print("=" * 65)
print(f"{'Modèle':<25} {'R²':>8} {'MAE':>10} {'MAE%':>8} {'CV R² (5-fold)':>16}")
print("-" * 65)

results = {}

for name, estimator in models.items():
    pipeline = build_pipeline(estimator)
    
    # sample_weight uniquement pour les modèles qui le supportent
    if name in ["Random Forest", "Gradient Boosting"]:
        # Récupérer le nom du dernier step (l'estimateur)
        estimator_name = pipeline.steps[-1][0]
        pipeline.fit(X_train, y_train, **{f"{estimator_name}__sample_weight": sample_weights})
    else:
        pipeline.fit(X_train, y_train)
    
    y_pred = pipeline.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mae_pct = mae / y_test.mean() * 100
    
    # Cross-validation (sans sample_weight pour simplifier la comparaison)
    cv_scores = cross_val_score(pipeline, X, y, cv=5, scoring="r2", n_jobs=-1)
    cv_mean = cv_scores.mean()
    
    results[name] = {
        "pipeline": pipeline,
        "r2": r2,
        "mae": mae,
        "mae_pct": mae_pct,
        "cv_r2": cv_mean,
    }
    
    print(f"{name:<25} {r2:>8.4f} {mae:>10.2f} {mae_pct:>7.1f}% {cv_mean:>15.4f}")

print("=" * 65)

best_name = max(results, key=lambda k: results[k]["r2"])
best = results[best_name]

print(f"\n🏆 Meilleur modèle : {best_name}")
print(f"   R²  : {best['r2']:.4f}")
print(f"   MAE : {best['mae']:.2f} passages/semaine (±{best['mae_pct']:.1f}%)")
print(f"   CV R² (5-fold) : {best['cv_r2']:.4f}")

joblib.dump({"model": best["pipeline"], "name": best_name}, MODEL_PATH)
print(f"\n✓ Modèle '{best_name}' sauvegardé → {MODEL_PATH}")