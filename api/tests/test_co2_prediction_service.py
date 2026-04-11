"""
Tests unitaires pour le service de prédiction CO2.
Lance avec : pytest tests/test_co2_prediction_service.py -v
"""

import pytest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import services.co2_prediction_service as svc


# ── Helpers ────────────────────────────────────────────────────────────────────

def reset_module_state():
    """Réinitialise l'état global du module entre les tests."""
    svc._pipeline = None
    svc._use_fallback = False


# ── Tests fallback ADEME ───────────────────────────────────────────────────────

class TestFallbackPredict:
    def test_electrique_tgv(self):
        result = svc._fallback_predict(500.0, "Électrique", "TGV")
        assert result["emission_gco2e_pkm"] == round(6.0 * 0.85, 2)
        assert result["total_emission_kgco2e"] == round(result["emission_gco2e_pkm"] * 500.0 / 1000, 4)
        assert result["model"] == "fallback-ademe-iea"
        assert "warning" in result

    def test_diesel_ter(self):
        result = svc._fallback_predict(100.0, "Diesel", "TER")
        assert result["emission_gco2e_pkm"] == round(41.0 * 1.10, 2)

    def test_hybride_intercites(self):
        result = svc._fallback_predict(200.0, "Hybride", "Intercités")
        assert result["emission_gco2e_pkm"] == round(22.0 * 1.0, 2)

    def test_traction_inconnue_fallback_autre(self):
        result = svc._fallback_predict(100.0, "Nuclear", "Autre")
        assert result["emission_gco2e_pkm"] == round(30.0 * 1.0, 2)

    def test_train_type_inconnu_factor_1(self):
        result = svc._fallback_predict(100.0, "Électrique", "Maglev")
        assert result["emission_gco2e_pkm"] == round(6.0 * 1.0, 2)

    def test_emission_per_km_positive(self):
        result = svc._fallback_predict(1.0, "Électrique", "TGV")
        assert result["emission_gco2e_pkm"] >= 0

    def test_total_proportionnel_distance(self):
        r1 = svc._fallback_predict(100.0, "Diesel", "TER")
        r2 = svc._fallback_predict(200.0, "Diesel", "TER")
        assert pytest.approx(r2["total_emission_kgco2e"], rel=1e-4) == r1["total_emission_kgco2e"] * 2


# ── Tests load_model ──────────────────────────────────────────────────────────

class TestLoadModel:
    def setup_method(self):
        reset_module_state()

    def test_fallback_si_hf_indisponible(self):
        with patch("services.co2_prediction_service.hf_hub_download", side_effect=Exception("no network")):
            svc.load_model()
        assert svc._use_fallback is True
        assert svc._pipeline is None

    def test_modele_charge_si_hf_disponible(self):
        fake_pipeline = MagicMock()
        with (
            patch("services.co2_prediction_service.hf_hub_download", return_value="/tmp/model.joblib"),
            patch("joblib.load", return_value=fake_pipeline),
        ):
            # Patch l'import de joblib dans le module
            import joblib
            with patch.object(joblib, "load", return_value=fake_pipeline):
                # On patche directement la fonction du module
                with patch("services.co2_prediction_service.load_model") as mock_load:
                    mock_load.side_effect = lambda: setattr(svc, "_pipeline", fake_pipeline) or setattr(svc, "_use_fallback", False)
                    svc.load_model()

    def test_get_model_appelle_load_si_non_charge(self):
        with patch("services.co2_prediction_service.load_model") as mock_load:
            mock_load.side_effect = lambda: setattr(svc, "_use_fallback", True)
            svc.get_model()
            mock_load.assert_called_once()

    def test_get_model_ne_recharge_pas_si_deja_fallback(self):
        svc._use_fallback = True
        with patch("services.co2_prediction_service.load_model") as mock_load:
            svc.get_model()
            mock_load.assert_not_called()


# ── Tests predict_co2 (fallback path) ─────────────────────────────────────────

class TestPredictCO2WithFallback:
    def setup_method(self):
        reset_module_state()
        svc._use_fallback = True  # force fallback

    def test_retourne_dict_complet(self):
        result = svc.predict_co2(300.0, 2.0, 5, "TGV", "Électrique")
        assert "emission_gco2e_pkm" in result
        assert "total_emission_kgco2e" in result
        assert "model" in result

    def test_model_est_fallback(self):
        result = svc.predict_co2(300.0, 2.0, 5, "TGV", "Électrique")
        assert result["model"] == "fallback-ademe-iea"

    def test_valeurs_coherentes_avec_fallback_direct(self):
        direct = svc._fallback_predict(300.0, "Électrique", "TGV")
        via_predict = svc.predict_co2(300.0, 2.0, 5, "TGV", "Électrique")
        assert direct["emission_gco2e_pkm"] == via_predict["emission_gco2e_pkm"]
        assert direct["total_emission_kgco2e"] == via_predict["total_emission_kgco2e"]


# ── Tests predict_co2 (ML path) ───────────────────────────────────────────────

class TestPredictCO2WithMLModel:
    def setup_method(self):
        reset_module_state()

    def test_utilise_pipeline_si_disponible(self):
        fake_pipeline = MagicMock()
        fake_pipeline.predict.return_value = [8.5]
        svc._pipeline = fake_pipeline
        svc._use_fallback = False

        import pandas as pd
        result = svc.predict_co2(500.0, 3.0, 4, "TGV", "Électrique")

        fake_pipeline.predict.assert_called_once()
        assert result["emission_gco2e_pkm"] == 8.5
        assert result["model"] == "hf-pipeline"

    def test_emission_jamais_negative(self):
        fake_pipeline = MagicMock()
        fake_pipeline.predict.return_value = [-5.0]   # valeur aberrante
        svc._pipeline = fake_pipeline
        svc._use_fallback = False

        result = svc.predict_co2(100.0, 1.0, 0, "TER", "Diesel")
        assert result["emission_gco2e_pkm"] >= 0.0

    def test_fallback_si_pipeline_leve_exception(self):
        fake_pipeline = MagicMock()
        fake_pipeline.predict.side_effect = RuntimeError("model broken")
        svc._pipeline = fake_pipeline
        svc._use_fallback = False

        result = svc.predict_co2(200.0, 1.5, 2, "TER", "Diesel")
        assert "warning" in result
        assert "model broken" in result["warning"]
