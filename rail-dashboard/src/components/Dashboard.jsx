import React, { useState, useEffect } from 'react'
import axios from 'axios'
import StatsCard from './StatsCard'
import ChartCard from './ChartCard'
import SearchTrips from './SearchTrips'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

function Dashboard() {
  const [overview, setOverview] = useState(null)
  const [countryStats, setCountryStats] = useState([])
  const [trainTypeStats, setTrainTypeStats] = useState([])
  const [tractionStats, setTractionStats] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetchData()
  }, [])

  const fetchData = async () => {
    try {
      setLoading(true)
      const [overviewRes, countryRes, trainTypeRes, tractionRes] = await Promise.all([
        axios.get(`${API_URL}/api/stats/overview`),
        axios.get(`${API_URL}/api/stats/by-country`),
        axios.get(`${API_URL}/api/stats/by-train-type`),
        axios.get(`${API_URL}/api/stats/by-traction`)
      ])

      setOverview(overviewRes.data)
      setCountryStats(countryRes.data)
      setTrainTypeStats(trainTypeRes.data)
      setTractionStats(tractionRes.data)
      setError(null)
    } catch (err) {
      setError('Erreur de connexion à l\'API: ' + err.message)
      console.error(err)
    } finally {
      setLoading(false)
    }
  }

  if (loading) return <div className="loading">⏳ Chargement des données...</div>
  if (error) return <div className="error">{error}</div>

  return (
    <div className="dashboard">
      {/* KPIs */}
      <div className="stats-grid">
        <StatsCard 
          title="Total Trajets" 
          value={overview?.total_trips?.toLocaleString() || '0'} 
          icon="🚆"
        />
        <StatsCard 
          title="Routes" 
          value={overview?.total_routes?.toLocaleString() || '0'} 
          icon="🛤️"
        />
        <StatsCard 
          title="Distance Totale" 
          value={`${overview?.total_distance_km?.toLocaleString() || '0'} km`} 
          icon="📏"
        />
        <StatsCard 
          title="Émissions CO2" 
          value={`${overview?.total_emissions_kg?.toLocaleString() || '0'} kg`} 
          icon="🌍"
        />
        <StatsCard 
          title="Distance Moyenne" 
          value={`${overview?.avg_distance_km?.toFixed(1) || '0'} km`} 
          icon="📊"
        />
        <StatsCard 
          title="Durée Moyenne" 
          value={`${overview?.avg_duration_h?.toFixed(2) || '0'} h`} 
          icon="⏱️"
        />
      </div>

      {/* Charts */}
      <div className="charts-grid">
        <ChartCard 
          title="📍 Trajets par Pays" 
          data={countryStats.slice(0, 10)} 
          dataKey="trip_count"
          nameKey="country"
        />
        <ChartCard 
          title="🚄 Trajets par Type de Train" 
          data={trainTypeStats.slice(0, 8)} 
          dataKey="trip_count"
          nameKey="train_type"
        />
        <ChartCard 
          title="⚡ Émissions par Traction" 
          data={tractionStats} 
          dataKey="avg_emission_per_km"
          nameKey="traction"
        />
      </div>

      {/* Search */}
      <SearchTrips apiUrl={API_URL} />
    </div>
  )
}

export default Dashboard