# test_train_model.py

import pytest
from unittest.mock import patch
import os 
import sys
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)
import train_model

class TestTrainModelPipeline:

    @patch("train_model.extract_data")
    @patch("train_model.train_model")
    @patch("train_model.use_model")
    def test_train_model_pipeline_success(self, mock_use_model, mock_train_model, mock_extract_data):
        mock_extract_data.extract.return_value = None
        mock_train_model.train.return_value = None
        mock_use_model.summary_model.return_value = {
            "r2": 0.85,
            "mae": 5.2,
            "mae_pct": 12.5,
            "y_test": [1, 2, 3],
            "y_pred": [1.5, 2.1, 2.8]
        }
        
        result = train_model.train_model_pipeline()
        
        mock_extract_data.extract.assert_called_once()
        mock_train_model.train.assert_called_once()
        mock_use_model.summary_model.assert_called_once()
        
        assert "r2" in result
        assert "mae" in result
        assert "mae_pct" in result
        assert result["r2"] == 0.85
        assert result["mae"] == 5.2
        assert "y_test" not in result
        assert "y_pred" not in result

    @patch("train_model.extract_data")
    @patch("train_model.train_model")
    @patch("train_model.use_model")
    def test_train_model_pipeline_with_numpy_values(self, mock_use_model, mock_train_model, mock_extract_data):
        
        import numpy as np
        
        mock_use_model.summary_model.return_value = {
            "r2": np.float64(0.95),
            "mae": np.float32(3.7),
            "mae_pct": 8.2,
            "y_test": np.array([1, 2, 3]),
            "y_pred": np.array([1.1, 2.2, 2.9])
        }
        
        result = train_model.train_model_pipeline()
        
        assert isinstance(result["r2"], float)
        assert isinstance(result["mae"], float)
        assert result["r2"] == 0.95
        assert result["mae"] == pytest.approx(3.7)
        assert result["mae_pct"] == 8.2

    @patch("train_model.extract_data")
    @patch("train_model.train_model")
    @patch("train_model.use_model")
    def test_train_model_pipeline_extraction_failure(self, mock_use_model, mock_train_model, mock_extract_data):
        
        mock_extract_data.extract.side_effect = Exception("Erreur de connexion à la base")
        
        with pytest.raises(Exception, match="Erreur de connexion à la base"):
            train_model.train_model_pipeline()
        
        mock_train_model.train.assert_not_called()
        mock_use_model.summary_model.assert_not_called()

    @patch("train_model.extract_data")
    @patch("train_model.train_model")
    @patch("train_model.use_model")
    def test_train_model_pipeline_training_failure(self, mock_use_model, mock_train_model, mock_extract_data):
        """Teste le comportement quand l'entraînement échoue"""
        
        mock_train_model.train.side_effect = Exception("Erreur d'entraînement")
        
        with pytest.raises(Exception, match="Erreur d'entraînement"):
            train_model.train_model_pipeline()
        
        mock_extract_data.extract.assert_called_once()
        mock_use_model.summary_model.assert_not_called()

    @patch("train_model.extract_data")
    @patch("train_model.train_model")
    @patch("train_model.use_model")
    def test_train_model_pipeline_print_output(self, mock_use_model, mock_train_model, mock_extract_data, capsys):
        """Teste les prints du pipeline"""
        
        mock_use_model.summary_model.return_value = {
            "r2": 0.88,
            "mae": 4.5,
            "mae_pct": 10.0,
            "y_test": [],
            "y_pred": []
        }
        
        train_model.train_model_pipeline()
        
        captured = capsys.readouterr()
        assert "=== Extraction ===" in captured.out
        assert "=== Entraînement ===" in captured.out
        assert "=== Test ===" in captured.out
        assert "=== Résumé métriques ===" in captured.out
        assert "r2: 0.88" in captured.out
        assert "mae: 4.5" in captured.out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])