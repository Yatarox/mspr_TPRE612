import React, { useState } from 'react'
import axios from 'axios'

function SearchTrips({ apiUrl }) {
  const [origin, setOrigin] = useState('')
  const [destination, setDestination] = useState('')
  const [trips, setTrips] = useState([])
  const [loading, setLoading] = useState(false)

  const handleSearch = async (e) => {
    e.preventDefault()
    setLoading(true)
    
    try {
      const params = {}
      if (origin) params.origin = origin
      if (destination) params.destination = destination
      params.limit = 20

      const res = await axios.get(`${apiUrl}/api/trips/search`, { params })
      setTrips(res.data)
    } catch (err) {
      console.error(err)
      alert('Erreur de recherche')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="search-section">
      <h2>🔍 Recherche de Trajets</h2>
      
      <form onSubmit={handleSearch} className="search-form">
        <div className="form-group">
          <label>Origine</label>
          <input 
            type="text" 
            placeholder="ex: Paris"
            value={origin}
            onChange={(e) => setOrigin(e.target.value)}
          />
        </div>
        
        <div className="form-group">
          <label>Destination</label>
          <input 
            type="text" 
            placeholder="ex: Lyon"
            value={destination}
            onChange={(e) => setDestination(e.target.value)}
          />
        </div>
        
        <button type="submit" className="search-button">
          {loading ? '⏳ Recherche...' : '🔎 Rechercher'}
        </button>
      </form>

      {trips.length > 0 && (
        <table className="trips-table">
          <thead>
            <tr>
              <th>Origine</th>
              <th>Destination</th>
              <th>Route</th>
              <th>Distance (km)</th>
              <th>Durée (h)</th>
              <th>Type Train</th>
              <th>CO2 (kg)</th>
            </tr>
          </thead>
          <tbody>
            {trips.map((trip, idx) => (
              <tr key={idx}>
                <td>{trip.origin} ({trip.origin_country})</td>
                <td>{trip.destination} ({trip.destination_country})</td>
                <td>{trip.route_name}</td>
                <td>{trip.distance_km?.toFixed(1)}</td>
                <td>{trip.duration_h?.toFixed(2)}</td>
                <td>{trip.train_type}</td>
                <td>{trip.total_emission_kgco2e?.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

export default SearchTrips