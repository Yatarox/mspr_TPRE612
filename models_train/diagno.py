import pandas as pd
import numpy as np
import joblib

artifact = joblib.load("models/frequency_model.joblib")
model    = artifact["model"]

rf          = model.named_steps["reg"]
prep        = model.named_steps["prep"]
feature_names = (
    ["distance_km", "duration_h"]
    + list(prep.named_transformers_["cat"]
           .get_feature_names_out(["train_type", "traction", "service_type", "origin_country", "destination_country"]))
)

importances = pd.Series(rf.feature_importances_, index=feature_names)
print("Top 20 features qui expliquent la fréquence :")
print(importances.sort_values(ascending=False).head(20).round(4).to_string())