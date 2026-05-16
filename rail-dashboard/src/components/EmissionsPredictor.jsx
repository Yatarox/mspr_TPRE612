import React, { useState } from 'react'
import axios from 'axios'

const EmissionsPredictor = ({ apiUrl }) => {
  const [formData, setFormData] = useState({
    distance_km: '',
    duration_h: '',
    nb_stops: 0,
    train_type: '',
    traction: ''
  })
  const [prediction, setPrediction] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const trainTypes = ['TGV', 'TER', 'Intercités', 'Eurostar', 'Thalys', 'RER', 'Transilien', 'ICE', 'Regional Train', 'High Speed Train']
  const tractions = ['Électrique', 'Diesel', 'Bi-mode', 'Hydrogène']

  const handleChange = (e) => {
    const { name, value } = e.target
    setFormData(prev => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    setPrediction(null)

    try {
      const params = {
        distance_km: parseFloat(formData.distance_km),
        duration_h: parseFloat(formData.duration_h),
        nb_stops: parseInt(formData.nb_stops) || 0,
        train_type: formData.train_type,
        traction: formData.traction
      }

      const response = await axios.get(`${apiUrl}/api/predict`, { params })
      setPrediction(response.data)
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Erreur lors de la prédiction')
    } finally {
      setLoading(false)
    }
  }

  const handleReset = () => {
    setFormData({
      distance_km: '',
      duration_h: '',
      nb_stops: 0,
      train_type: '',
      traction: ''
    })
    setPrediction(null)
    setError('')
  }

  return (
    <div className="predictor-card" data-testid="predictor-card">
      <form onSubmit={handleSubmit} className="predictor-form" data-testid="predictor-form">
        <div className="form-row">
          <div className="form-group">
            <label>Distance (km) *</label>
            <input
              type="number"
              name="distance_km"
              value={formData.distance_km}
              onChange={handleChange}
              placeholder="Ex: 500"
              step="0.1"
              required
              data-testid="predictor-distance"
            />
          </div>

          <div className="form-group">
            <label>Durée (heures) *</label>
            <input
              type="number"
              name="duration_h"
              value={formData.duration_h}
              onChange={handleChange}
              placeholder="Ex: 2.5"
              step="0.1"
              required
              data-testid="predictor-duration"
            />
          </div>

          <div className="form-group">
            <label>Nombre d'arrêts</label>
            <input
              type="number"
              name="nb_stops"
              value={formData.nb_stops}
              onChange={handleChange}
              placeholder="Ex: 3"
              min="0"
              data-testid="predictor-stops"
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>Type de train *</label>
            <select 
              name="train_type" 
              value={formData.train_type} 
              onChange={handleChange} 
              required
              data-testid="predictor-train-type"
            >
              <option value="">Sélectionnez...</option>
              {trainTypes.map(type => (
                <option key={type} value={type}>{type}</option>
              ))}
            </select>
          </div>

          <div className="form-group">
            <label>Traction *</label>
            <select 
              name="traction" 
              value={formData.traction} 
              onChange={handleChange} 
              required
              data-testid="predictor-traction"
            >
              <option value="">Sélectionnez...</option>
              {tractions.map(traction => (
                <option key={traction} value={traction}>{traction}</option>
              ))}
            </select>
          </div>

          <div className="form-actions">
            <button 
              type="submit" 
              className="primary-button" 
              disabled={loading}
              data-testid="predict-submit"
            >
              {loading ? '🔮 Calcul en cours...' : '🔮 Prédire les émissions'}
            </button>
            <button 
              type="button" 
              className="secondary-button" 
              onClick={handleReset}
              data-testid="predict-reset"
            >
              Réinitialiser
            </button>
          </div>
        </div>
      </form>

      {error && (
        <div className="predictor-error" data-testid="predictor-error">
          ⚠️ {error}
        </div>
      )}

      {prediction && (
        <div className="predictor-result" data-testid="predictor-result">
          <h3>📊 Résultat de la prédiction</h3>
          <div className="result-grid">
            <div className="result-item">
              <span className="result-label">Émissions par km</span>
              <span className="result-value" data-testid="prediction-perkm">
                {prediction.emission_gco2e_pkm?.toFixed(2) || 'N/A'} g CO₂/km
              </span>
            </div>
            <div className="result-item">
              <span className="result-label">Émissions totales</span>
              <span className="result-value highlight" data-testid="prediction-total">
                {prediction.total_emission_kgco2e?.toFixed(2) || 'N/A'} kg CO₂
              </span>
            </div>
            {prediction.frequency_per_week && (
              <div className="result-item">
                <span className="result-label">Fréquence prédite</span>
                <span className="result-value" data-testid="prediction-frequency">
                  {prediction.frequency_per_week.toFixed(1)} trains/semaine
                </span>
              </div>
            )}
            {prediction.model && (
              <div className="result-item">
                <span className="result-label">Modèle IA</span>
                <span className="result-value" data-testid="prediction-model">
                  {prediction.model}
                </span>
              </div>
            )}
            {prediction.warning && (
              <div className="result-item full-width">
                <span className="result-label warning">⚠️ Attention</span>
                <span className="result-value" data-testid="prediction-warning">
                  {prediction.warning}
                </span>
              </div>
            )}
          </div>
          <div className="result-comparison">
            <small>Comparaison : Un trajet Paris-Lyon (465km) émet environ 4.7 kg CO₂ en TGV électrique</small>
          </div>
        </div>
      )}
    </div>
  )
}

export default EmissionsPredictor