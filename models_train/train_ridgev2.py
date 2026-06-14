import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split, cross_val_score, RandomizedSearchCV
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
import warnings
import os
warnings.filterwarnings('ignore')

DATA_PATH = "data/trips_freq.csv"
MODEL_PATH = "models/frequency_model_best.joblib"

os.makedirs("models", exist_ok=True)
print("=" * 70)
print("CHARGEMENT DES DONNÉES")
print("=" * 70)

df = pd.read_csv(DATA_PATH)

# Nettoyage
df = df[df['distance_km'] > 0]
df = df[df['duration_h'] > 0]
df = df[df['frequency_per_week'] > 0]

print(f"📊 Données chargées : {len(df)} trajets")

# Feature engineering
df['speed_kmh'] = df['distance_km'] / df['duration_h']
df['is_night'] = (df['service_type'] == 'NUIT').astype(int)
df['distance_night'] = df['distance_km'] * df['is_night']

NUM_FEATURES = ["distance_km", "duration_h", "speed_kmh", "is_night", "distance_night"]
CAT_FEATURES = ["traction"]
TARGET = "frequency_per_week"

X = df[NUM_FEATURES + CAT_FEATURES]
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=df['service_type']
)

print(f"Train: {len(X_train)} | Test: {len(X_test)}")

# Poids pour rééquilibrer JOUR/NUIT
n_jour = len(df[df['service_type'] == 'JOUR'])
n_nuit = len(df[df['service_type'] == 'NUIT'])
sample_weights = X_train['is_night'].map({0: 1, 1: n_jour / n_nuit}).values

print(f"\n🎯 Rééquilibrage : poids NUIT = {n_jour / n_nuit:.2f}")

# Préprocesseur
preprocessor = ColumnTransformer([
    ("num", StandardScaler(), NUM_FEATURES),
    ("cat", OneHotEncoder(handle_unknown="ignore", drop='first'), CAT_FEATURES),
])

print("\n" + "=" * 70)
print("COMPARAISON DES MODÈLES")
print("=" * 70)

models = {
    "Ridge": Ridge(alpha=1.0),
    "Lasso": Lasso(alpha=0.01),
    "ElasticNet": ElasticNet(alpha=0.01, l1_ratio=0.5),
    "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
    "AdaBoost": AdaBoostRegressor(n_estimators=100, random_state=42),
    "XGBoost": XGBRegressor(n_estimators=100, random_state=42, n_jobs=-1, verbosity=0),
    "LightGBM": LGBMRegressor(n_estimators=100, random_state=42, n_jobs=-1, verbose=-1),
}

models_supporting_weights = ["Random Forest", "Gradient Boosting", "AdaBoost", "XGBoost", "LightGBM"]
results = []

print(f"\n{'Modèle':<20} {'R² test':>10} {'MAE':>10} {'MAE%':>8} {'RMSE':>10} {'CV R²':>10}")
print("-" * 70)

for name, estimator in models.items():
    pipeline = make_pipeline(preprocessor, estimator)
    
    if name in models_supporting_weights:
        pipeline.fit(X_train, y_train, **{f"{estimator.__class__.__name__.lower()}__sample_weight": sample_weights})
    else:
        pipeline.fit(X_train, y_train)
    
    y_pred = pipeline.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mae_pct = mae / y_test.mean() * 100
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    cv_scores = cross_val_score(pipeline, X, y, cv=5, scoring="r2", n_jobs=-1)
    cv_mean = cv_scores.mean()
    
    results.append({
        "name": name,
        "pipeline": pipeline,
        "estimator": estimator,
        "r2": r2,
        "mae": mae,
        "mae_pct": mae_pct,
        "rmse": rmse,
        "cv_r2": cv_mean
    })
    
    print(f"{name:<20} {r2:>10.4f} {mae:>10.2f} {mae_pct:>7.1f}% {rmse:>10.2f} {cv_mean:>10.4f}")

print("=" * 70)

best = max(results, key=lambda x: x["r2"])
print(f"\n🏆 MEILLEUR MODÈLE : {best['name']}")
print(f"   R² test  : {best['r2']:.4f}")
print(f"   MAE      : {best['mae']:.2f} passages/semaine")
print(f"   MAE%     : {best['mae_pct']:.1f}%")
print(f"   RMSE     : {best['rmse']:.2f}")
print(f"   CV R²    : {best['cv_r2']:.4f}")

print("\n" + "=" * 70)
print("OPTIMISATION DU MEILLEUR MODÈLE")
print("=" * 70)

if best['name'] == "XGBoost":
    param_grid = {
        'xgbregressor__n_estimators': [50, 100, 200],
        'xgbregressor__max_depth': [3, 5, 7],
        'xgbregressor__learning_rate': [0.01, 0.05, 0.1],
        'xgbregressor__subsample': [0.8, 1.0],
    }
    estimator = XGBRegressor(random_state=42, n_jobs=-1, verbosity=0)
    
elif best['name'] == "LightGBM":
    param_grid = {
        'lgbmregressor__n_estimators': [50, 100, 200],
        'lgbmregressor__max_depth': [3, 5, 7],
        'lgbmregressor__learning_rate': [0.01, 0.05, 0.1],
        'lgbmregressor__subsample': [0.8, 1.0],
    }
    estimator = LGBMRegressor(random_state=42, n_jobs=-1, verbose=-1)
    
elif best['name'] == "Random Forest":
    param_grid = {
        'randomforestregressor__n_estimators': [50, 100, 200],
        'randomforestregressor__max_depth': [5, 10, None],
        'randomforestregressor__min_samples_split': [2, 5, 10],
    }
    estimator = RandomForestRegressor(random_state=42, n_jobs=-1)
    
else:
    param_grid = {
        'gradientboostingregressor__n_estimators': [50, 100, 200],
        'gradientboostingregressor__max_depth': [3, 5, 7],
        'gradientboostingregressor__learning_rate': [0.01, 0.05, 0.1],
        'gradientboostingregressor__subsample': [0.8, 1.0],
    }
    estimator = GradientBoostingRegressor(random_state=42)

pipeline = make_pipeline(preprocessor, estimator)

search = RandomizedSearchCV(
    pipeline, 
    param_grid, 
    n_iter=20, 
    cv=5, 
    scoring='r2',
    random_state=42,
    n_jobs=-1,
    verbose=0
)

estimator_name = estimator.__class__.__name__.lower()
search.fit(X_train, y_train, **{f"{estimator_name}__sample_weight": sample_weights})

print(f"\n📊 Meilleurs paramètres trouvés :")
for param, value in search.best_params_.items():
    print(f"   {param}: {value}")

print(f"\n📈 Performance après optimisation :")
best_optimized = search.best_estimator_
y_pred_opt = best_optimized.predict(X_test)
r2_opt = r2_score(y_test, y_pred_opt)
mae_opt = mean_absolute_error(y_test, y_pred_opt)
mae_pct_opt = mae_opt / y_test.mean() * 100

print(f"   R² : {best['r2']:.4f} → {r2_opt:.4f} (+{r2_opt - best['r2']:.4f})")
print(f"   MAE: {best['mae']:.2f} → {mae_opt:.2f} ({mae_opt - best['mae']:+.2f})")

if r2_opt > best['r2']:
    joblib.dump({"model": best_optimized, "name": f"{best['name']}_optimized"}, MODEL_PATH)
    print(f"\n✓ Modèle optimisé sauvegardé → {MODEL_PATH}")
    final_model = best_optimized
else:
    joblib.dump({"model": best["pipeline"], "name": best['name']}, MODEL_PATH)
    print(f"\n✓ Modèle original sauvegardé → {MODEL_PATH}")
    final_model = best["pipeline"]

print("\n" + "=" * 70)
print("VISUALISATIONS POUR LE RAPPORT")
print("=" * 70)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].scatter(y_test, y_pred_opt, alpha=0.5, edgecolors='k', s=30)
axes[0].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
axes[0].set_xlabel('Valeurs réelles')
axes[0].set_ylabel('Prédictions')
axes[0].set_title(f'Prédictions vs Réel - R² = {r2_opt:.3f}')

errors = y_test - y_pred_opt
axes[1].hist(errors, bins=30, edgecolor='black', alpha=0.7)
axes[1].axvline(0, color='r', linestyle='--', linewidth=2)
axes[1].set_xlabel('Erreur (réel - prédit)')
axes[1].set_ylabel('Fréquence')
axes[1].set_title(f'Distribution des erreurs - MAE = {mae_opt:.2f}')

plt.tight_layout()
plt.savefig("models_train/model_performance.png", dpi=150)
print("📊 Graphique sauvegardé : models_train/model_performance.png")

# Feature importance - Version corrigée
try:
    # Récupérer le modèle depuis le pipeline
    if hasattr(final_model, 'named_steps'):
        model = final_model.named_steps[final_model.steps[-1][0]]
    else:
        model = final_model.steps[-1][1]
    
    if hasattr(model, 'feature_importances_'):
        # Compter le nombre de features après transformation
        preprocessor.fit(X_train)
        feature_names = NUM_FEATURES.copy()
        
        # Ajouter les noms des features catégorielles encodées
        cat_encoder = preprocessor.named_transformers_['cat']
        if hasattr(cat_encoder, 'get_feature_names_out'):
            cat_features = cat_encoder.get_feature_names_out(CAT_FEATURES).tolist()
        else:
            cat_features = [f"{CAT_FEATURES[0]}_{cat}" for cat in cat_encoder.categories_[0][1:]]
        
        all_feature_names = feature_names + cat_features
        
        importances = model.feature_importances_
        
        # Ajuster la taille si nécessaire
        if len(importances) != len(all_feature_names):
            print(f"⚠️ Nombre de features incohérent : {len(importances)} vs {len(all_feature_names)}")
            # Prendre le minimum
            min_len = min(len(importances), len(all_feature_names))
            importances = importances[:min_len]
            all_feature_names = all_feature_names[:min_len]
        
        indices = np.argsort(importances)[::-1]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(range(len(importances)), importances[indices])
        ax.set_yticks(range(len(importances)))
        ax.set_yticklabels([all_feature_names[i] for i in indices])
        ax.set_xlabel('Importance')
        ax.set_title(f'Importance des features - {best["name"]}')
        ax.invert_yaxis()
        
        plt.tight_layout()
        plt.savefig("models_train/feature_importance.png", dpi=150)
        print("📊 Feature importance sauvegardée : models_train/feature_importance.png")
    else:
        print("ℹ️ Le modèle ne supporte pas la feature importance")
        
except Exception as e:
    print(f"⚠️ Erreur lors de la feature importance : {e}")

print("\n✅ Entraînement terminé !")