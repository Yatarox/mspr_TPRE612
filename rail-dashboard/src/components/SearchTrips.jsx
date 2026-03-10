import React, { useMemo, useState } from 'react'
import axios from 'axios'

const formatNumber = (value, digits = 1) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: digits
  }).format(Number(value) || 0)

function SearchTrips({ apiUrl }) {
  const [origin, setOrigin] = useState('')
  const [destination, setDestination] = useState('')
  const [trips, setTrips] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [hasSearched, setHasSearched] = useState(false)

  const resultLabel = useMemo(() => {
    if (!hasSearched) return 'Recherche prête'
    return `${trips.length} résultat${trips.length > 1 ? 's' : ''}`
  }, [hasSearched, trips.length])

  const handleSearch = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')

    try {
      const params = { limit: 20 }

      if (origin.trim()) params.origin = origin.trim()
      if (destination.trim()) params.destination = destination.trim()

      const res = await axios.get(`${apiUrl}/api/trips/search`, { params })
      setTrips(Array.isArray(res.data) ? res.data : [])
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
    setTrips([])
    setError('')
    setHasSearched(false)
  }

  return (
    <section className="search-section">
      <div className="search-header">
        <div>
          <span className="section-pill">Recherche avancée</span>
          <h2>Explorer les trajets</h2>
          <p>
            Recherche rapide par origine et destination avec restitution claire des
            résultats opérationnels.
          </p>
        </div>

        <div className="search-status-badge">{resultLabel}</div>
      </div>

      <form onSubmit={handleSearch} className="search-form">
        <div className="form-group">
          <label htmlFor="origin">Origine</label>
          <input
            id="origin"
            type="text"
            placeholder="Ex. Paris"
            value={origin}
            onChange={(e) => setOrigin(e.target.value)}
          />
        </div>

        <div className="form-group">
          <label htmlFor="destination">Destination</label>
          <input
            id="destination"
            type="text"
            placeholder="Ex. Lyon"
            value={destination}
            onChange={(e) => setDestination(e.target.value)}
          />
        </div>

        <div className="search-actions">
          <button type="submit" className="primary-button" disabled={loading}>
            {loading ? 'Recherche...' : 'Rechercher'}
          </button>

          <button
            type="button"
            className="secondary-button"
            onClick={handleReset}
            disabled={loading}
          >
            Réinitialiser
          </button>
        </div>
      </form>

      {error ? <div className="inline-error">Impossible d’effectuer la recherche : {error}</div> : null}

      {hasSearched && !loading && trips.length === 0 && !error ? (
        <div className="empty-state">
          <div>
            <h3>Aucun trajet trouvé</h3>
            <p>Modifie les critères de recherche pour élargir les résultats.</p>
          </div>
        </div>
      ) : null}

      {trips.length > 0 ? (
        <div className="search-results">
          <div className="table-wrapper">
            <table className="trips-table">
              <thead>
                <tr>
                  <th>Origine</th>
                  <th>Destination</th>
                  <th>Route</th>
                  <th>Distance</th>
                  <th>Durée</th>
                  <th>Type de train</th>
                  <th>CO2</th>
                </tr>
              </thead>
              <tbody>
                {trips.map((trip, idx) => (
                  <tr
                    key={`${trip.route_name || 'route'}-${trip.origin || 'origin'}-${trip.destination || 'destination'}-${idx}`}
                  >
                    <td>
                      <span className="city">{trip.origin || '—'}</span>
                      <span className="country">{trip.origin_country || 'Pays inconnu'}</span>
                    </td>
                    <td>
                      <span className="city">{trip.destination || '—'}</span>
                      <span className="country">{trip.destination_country || 'Pays inconnu'}</span>
                    </td>
                    <td>
                      <span className="route-name">{trip.route_name || '—'}</span>
                    </td>
                    <td className="number-cell">{formatNumber(trip.distance_km, 1)} km</td>
                    <td className="number-cell">{formatNumber(trip.duration_h, 2)} h</td>
                    <td>
                      <span className="data-pill">{trip.train_type || 'Non renseigné'}</span>
                    </td>
                    <td className="number-cell">
                      {formatNumber(trip.total_emission_kgco2e, 2)} kg
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </section>
  )
}

export default SearchTrips