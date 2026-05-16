# ruff: noqa: F401

import sys
import os
import numpy as np
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from train_model_script import extract_data, use_model, train_model

def train_model_pipeline():
    print("=== Extraction ===")
    extract_data.extract()
    print("=== Entraînement ===")
    train_model.train()
    print("=== Test ===")
    metrics = use_model.summary_model()
    print("=== Résumé métriques ===")
    for k, v in metrics.items():
        if k not in ("y_test", "y_pred"):
            print(f"{k}: {v}")
    return {k: float(v) if isinstance(v, (int, float, np.floating, np.integer)) else v
            for k, v in metrics.items() if k not in ("y_test", "y_pred")}

if __name__ == "__main__":
    train_model_pipeline()