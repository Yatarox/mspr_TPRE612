import React, { useMemo, useState } from 'react'
import axios from 'axios'

const formatNumber = (value, digits = 1) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: digits
  }).format(Number(value) || 0)

const getValue = (object, keys, fallback = '—') => {
  for (const key of keys) {
    const value = object?.[key]
    if (value !== undefined && value !== null && value !== '') {
      return value
    }
  }
  return fallback
}

const formatFrequency = (trip) => {
  const value = getValue(
    trip,
    ['frequency_per_week', 'frequency', 'service_frequency', 'headway_label', 'trip_frequency'],
    null
  )

  if (value === null) return 'Non renseignée'
  if (typeof value === 'number') return `${value} / semaine`

  const numeric = Number(value)
  if (!Number.isNaN(numeric) && String(value).trim() !== '') {
    return `${numeric} / semaine`
  }

  return String(value)
}

const normalizeDayNightLabel = (trip) => {
  const raw = getValue(
    trip,
    ['service_type', 'day_night_type', 'service_period', 'trip_period', 'journey_period', 'time_of_day', 'service_label'],
    ''
  )

  if (!raw) return 'Non renseigné'

  const normalized = String(raw).toLowerCase()
  if (normalized.includes('night') || normalized.includes('nuit') || normalized === 'n') return '🌙 Train de nuit'
  if (normalized.includes('day') || normalized.includes('jour') || normalized === 'd') return '☀️ Train de jour'
  return raw
}

function SearchTrips({ apiUrl }) {
  const [origin, setOrigin] = useState('')
  const [destination, setDestination] = useState('')
  const [trainType, setTrainType] = useState('')
  const [minDistance, setMinDistance] = useState('')
  const [maxDistance, setMaxDistance] = useState('')
  const [limit, setLimit] = useState(100)
  const [trips, setTrips] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [hasSearched, setHasSearched] = useState(false)
  const [showAdvanced, setShowAdvanced] = useState(false)

  const resultLabel = useMemo(() => {
    if (!hasSearched) return 'Recherche prête'
    return `${trips.length} résultat${trips.length > 1 ? 's' : ''}`
  }, [hasSearched, trips.length])

  const handleSearch = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')

    try {
      const params = { limit }
      
      if (origin.trim()) params.origin = origin.trim()
      if (destination.trim()) params.destination = destination.trim()
      if (trainType) params.train_type = trainType
      if (minDistance) params.min_distance = parseFloat(minDistance)
      if (maxDistance) params.max_distance = parseFloat(maxDistance)

      const res = await axios.get(`${apiUrl}/api/trips/search`, { params })
      setTrips(Array.isArray(res.data) ? res.data : [])
      
      if (res.data.length === 0) {
        setError('Aucun trajet trouvé avec ces critères')
      }
    } catch (err) {
      const message =
        err?.response?.data?.detail ||
        err?.response?.data?.message ||
        err?.message ||
        'Erreur lors de la recherche.'
      setTrips([])
      setError(message)
    } finally {
      setHasSearched(true)
      setLoading(false)
    }
  }

  const handleReset = () => {
    setOrigin('')
    setDestination('')
    setTrainType('')
    setMinDistance('')
    setMaxDistance('')
    setLimit(100)
    setTrips([])
    setError('')
    setHasSearched(false)
  }

  const trainTypes = [
    'TGV',
    'TER',
    'Intercités',
    'Eurostar',
    'Thalys',
    'RER',
    'Transilien',
    'ICE',
    'Regional Train',
    'High Speed Train'
  ]

  return (
    <section className="search-section" data-testid="search-section">
      <div className="search-header">
        <div>
          <span className="section-pill">
            <span className="pill-icon">🔍</span>
            Recherche avancée
          </span>
          <h2>Explorer les trajets</h2>
          <p>Recherchez des trajets par origine, destination, type de train ou distance.</p>
        </div>
        <div className="search-status-badge" data-testid="search-status">
          {resultLabel}
        </div>
      </div>

      <form onSubmit={handleSearch} className="search-form" data-testid="search-form">
        <div className="form-row">
          <div className="form-group">
            <label htmlFor="origin">
              <span className="label-icon">🚉</span>
              Origine
            </label>
            <input 
              id="origin" 
              type="text" 
              placeholder="Ex: Paris, Lyon, Marseille..." 
              value={origin} 
              onChange={(e) => setOrigin(e.target.value)}
              className="search-input"
              data-testid="origin-input"
            />
          </div>

          <div className="form-group">
            <label htmlFor="destination">
              <span className="label-icon">📍</span>
              Destination
            </label>
            <input 
              id="destination" 
              type="text" 
              placeholder="Ex: Londres, Bruxelles..." 
              value={destination} 
              onChange={(e) => setDestination(e.target.value)}
              className="search-input"
              data-testid="destination-input"
            />
          </div>

          <div className="form-group">
            <label htmlFor="limit">
              <span className="label-icon">📋</span>
              Nombre de résultats
            </label>
            <select 
              id="limit" 
              value={limit} 
              onChange={(e) => setLimit(Number(e.target.value))}
              className="search-select"
              data-testid="limit-select"
            >
              <option value={20}>20 résultats</option>
              <option value={50}>50 résultats</option>
              <option value={100}>100 résultats</option>
              <option value={200}>200 résultats</option>
              <option value={500}>500 résultats</option>
            </select>
          </div>
        </div>

        <button 
          type="button" 
          className="advanced-toggle" 
          onClick={() => setShowAdvanced(!showAdvanced)}
          data-testid="advanced-toggle"
        >
          {showAdvanced ? '▼ Masquer les filtres avancés' : '▶ Afficher les filtres avancés'}
        </button>

        {showAdvanced && (
          <div className="advanced-filters" data-testid="advanced-filters">
            <div className="form-row">
              <div className="form-group">
                <label htmlFor="trainType">
                  <span className="label-icon">🚄</span>
                  Type de train
                </label>
                <select 
                  id="trainType" 
                  value={trainType} 
                  onChange={(e) => setTrainType(e.target.value)}
                  className="search-select"
                  data-testid="train-type-select"
                >
                  <option value="">Tous les types</option>
                  {trainTypes.map(type => (
                    <option key={type} value={type}>{type}</option>
                  ))}
                </select>
              </div>

              <div className="form-group">
                <label htmlFor="minDistance">
                  <span className="label-icon">📏</span>
                  Distance minimum (km)
                </label>
                <input 
                  id="minDistance" 
                  type="number" 
                  placeholder="0" 
                  value={minDistance} 
                  onChange={(e) => setMinDistance(e.target.value)}
                  className="search-input"
                  data-testid="min-distance-input"
                  min="0"
                  step="10"
                />
              </div>

              <div className="form-group">
                <label htmlFor="maxDistance">
                  <span className="label-icon">📏</span>
                  Distance maximum (km)
                </label>
                <input 
                  id="maxDistance" 
                  type="number" 
                  placeholder="1000" 
                  value={maxDistance} 
                  onChange={(e) => setMaxDistance(e.target.value)}
                  className="search-input"
                  data-testid="max-distance-input"
                  min="0"
                  step="10"
                />
              </div>
            </div>
          </div>
        )}

        <div className="search-actions">
          <button 
            type="submit" 
            className="primary-button" 
            disabled={loading}
            data-testid="search-button"
          >
            {loading ? (
              <>
                <span className="spinner"></span>
                Recherche en cours...
              </>
            ) : (
              <>
                🔍 Rechercher
              </>
            )}
          </button>
          <button 
            type="button" 
            className="secondary-button" 
            onClick={handleReset} 
            disabled={loading}
            data-testid="reset-button"
          >
            🗑️ Réinitialiser
          </button>
        </div>
      </form>

      {error && (
        <div className="inline-error" data-testid="search-error">
          <span className="error-icon">⚠️</span>
          {error}
        </div>
      )}

      {trips.length > 0 && (
        <div className="search-results" data-testid="search-results">
          <div className="results-header">
            <div className="results-count">
              <span className="count-badge">{trips.length}</span>
              trajet{trips.length > 1 ? 's' : ''} trouvé{trips.length > 1 ? 's' : ''}
            </div>
            <div className="results-actions">
              <button 
                className="icon-button-small" 
                onClick={() => {
                  const csv = convertToCSV(trips)
                  downloadCSV(csv, `trips_export_${new Date().toISOString().split('T')[0]}.csv`)
                }}
                title="Exporter en CSV"
                data-testid="export-csv-button"
              >
                📥 Exporter
              </button>
            </div>
          </div>

          <div className="table-wrapper">
            <table className="trips-table trips-table-extended" data-testid="trips-table">
              <thead>
                <tr>
                  <th>ID Trajet</th>
                  <th>Service</th>
                  <th>Origine</th>
                  <th>Destination</th>
                  <th>Route</th>
                  <th>Distance</th>
                  <th>Durée</th>
                  <th>Type train</th>
                  <th>Traction</th>
                  <th>Fréquence</th>
                  <th>Période</th>
                  <th>Horaires</th>
                  <th>Opérateur</th>
                  <th>CO₂</th>
                </tr>
              </thead>
              <tbody>
                {trips.map((trip, idx) => {
                  const routeName = getValue(trip, ['route_name', 'route_long_name', 'route_short_name'])
                  const trainTypeValue = getValue(trip, ['train_type'], 'Non renseigné')
                  const traction = getValue(trip, ['traction'], 'Non renseignée')
                  const frequency = formatFrequency(trip)
                  const operator = getValue(trip, ['agency_name'], 'Non renseigné')
                  const departureTime = getValue(trip, ['departure_time'], '')
                  const arrivalTime = getValue(trip, ['arrival_time'], '')
                  const serviceLabel = normalizeDayNightLabel(trip)
                  
                  const emissionPerKm = trip.total_emission_kgco2e && trip.distance_km 
                    ? (trip.total_emission_kgco2e / trip.distance_km * 1000).toFixed(1)
                    : null

                  return (
                    <tr key={`${trip.trip_id || trip.fact_sk || idx}`} className="trip-row" data-testid={`trip-row-${idx}`}>
                      <td data-label="ID Trajet">
                        <span className="data-pill">{trip.trip_id ?? '—'}</span>
                      </td>
                      <td data-label="Service">
                        {trip.service_label ?? trip.service_type ?? 'Non renseigné'}
                      </td>
                      <td data-label="Origine">
                        <span className="city">{trip.origin || '—'}</span>
                        <span className="country">{trip.origin_country || 'Pays inconnu'}</span>
                      </td>
                      <td data-label="Destination">
                        <span className="city">{trip.destination || '—'}</span>
                        <span className="country">{trip.destination_country || 'Pays inconnu'}</span>
                      </td>
                      <td data-label="Route">
                        <span className="route-name">{routeName}</span>
                      </td>
                      <td data-label="Distance" className="number-cell">
                        <strong>{formatNumber(trip.distance_km, 1)} km</strong>
                      </td>
                      <td data-label="Durée" className="number-cell">
                        {formatNumber(trip.duration_h, 2)} h
                      </td>
                      <td data-label="Type train">
                        <span className="train-type-badge">{trainTypeValue}</span>
                      </td>
                      <td data-label="Traction">
                        <span className="traction-badge">{traction}</span>
                      </td>
                      <td data-label="Fréquence">
                        {frequency}
                      </td>
                      <td data-label="Période">
                        <span className="neutral-pill">{serviceLabel}</span>
                      </td>
                      <td data-label="Horaires">
                        <div className="schedule-cell">
                          <span>🚂 {departureTime || '—'}</span>
                          <span>🏁 {arrivalTime || '—'}</span>
                        </div>
                      </td>
                      <td data-label="Opérateur">
                        {operator}
                      </td>
                      <td data-label="CO₂" className="number-cell">
                        <div className="co2-cell">
                          <span className="co2-value">{formatNumber(trip.total_emission_kgco2e, 2)} kg</span>
                          {emissionPerKm && (
                            <span className="co2-perkm">({emissionPerKm} g/km)</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  )
}

// Helper function to convert trips to CSV
function convertToCSV(trips) {
  const headers = ['ID Trajet', 'Service', 'Origine', 'Pays Origine', 'Destination', 'Pays Destination', 
                   'Route', 'Distance (km)', 'Durée (h)', 'Type Train', 'Traction', 'Fréquence', 
                   'Période', 'Départ', 'Arrivée', 'Opérateur', 'CO₂ (kg)']
  
  const rows = trips.map(trip => [
    trip.trip_id || '',
    trip.service_label || trip.service_type || '',
    trip.origin || '',
    trip.origin_country || '',
    trip.destination || '',
    trip.destination_country || '',
    getValue(trip, ['route_name', 'route_long_name', 'route_short_name']),
    trip.distance_km || '',
    trip.duration_h || '',
    getValue(trip, ['train_type'], ''),
    getValue(trip, ['traction'], ''),
    formatFrequency(trip),
    normalizeDayNightLabel(trip),
    getValue(trip, ['departure_time'], ''),
    getValue(trip, ['arrival_time'], ''),
    getValue(trip, ['agency_name'], ''),
    trip.total_emission_kgco2e || ''
  ])
  
  const csvContent = [headers, ...rows].map(row => row.join(',')).join('\n')
  return csvContent
}

function downloadCSV(csv, filename) {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  const url = URL.createObjectURL(blob)
  link.setAttribute('href', url)
  link.setAttribute('download', filename)
  link.style.visibility = 'hidden'
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

export default SearchTrips