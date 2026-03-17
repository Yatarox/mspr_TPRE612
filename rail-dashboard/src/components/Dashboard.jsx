import React, { useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import StatsCard from './StatsCard'
import ChartCard from './ChartCard'
import SearchTrips from './SearchTrips'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const formatInteger = (value) =>
  new Intl.NumberFormat('fr-FR').format(Number(value) || 0)

const formatDecimal = (value, digits = 1) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: digits
  }).format(Number(value) || 0)

const getErrorMessage = (error) => {
  if (typeof error?.response?.data?.detail === 'string') return error.response.data.detail
  if (typeof error?.response?.data?.message === 'string') return error.response.data.message
  if (typeof error?.message === 'string') return error.message
  return 'Une erreur inattendue est survenue.'
}

function Dashboard() {
  const [overview, setOverview] = useState(null)
  const [countryStats, setCountryStats] = useState([])
  const [trainTypeStats, setTrainTypeStats] = useState([])
  const [tractionStats, setTractionStats] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    fetchData()
  }, [])

  const fetchData = async () => {
    try {
      setLoading(true)
      setError('')

      const [overviewRes, countryRes, trainTypeRes, tractionRes] = await Promise.all([
        axios.get(`${API_URL}/api/stats/overview`),
        axios.get(`${API_URL}/api/stats/by-country`),
        axios.get(`${API_URL}/api/stats/by-train-type`),
        axios.get(`${API_URL}/api/stats/by-traction`)
      ])

      setOverview(overviewRes.data)
      setCountryStats(Array.isArray(countryRes.data) ? countryRes.data : [])
      setTrainTypeStats(Array.isArray(trainTypeRes.data) ? trainTypeRes.data : [])
      setTractionStats(Array.isArray(tractionRes.data) ? tractionRes.data : [])
    } catch (err) {
      setError(`Erreur de connexion à l’API : ${getErrorMessage(err)}`)
      console.error(err)
    } finally {
      setLoading(false)
    }
  }

  const sortedCountryStats = useMemo(
    () => [...countryStats].sort((a, b) => (b?.trip_count ?? 0) - (a?.trip_count ?? 0)),
    [countryStats]
  )

  const sortedTrainTypeStats = useMemo(
    () => [...trainTypeStats].sort((a, b) => (b?.trip_count ?? 0) - (a?.trip_count ?? 0)),
    [trainTypeStats]
  )

  const sortedTractionStats = useMemo(
    () =>
      [...tractionStats].sort(
        (a, b) => (b?.avg_emission_per_km ?? 0) - (a?.avg_emission_per_km ?? 0)
      ),
    [tractionStats]
  )

  const cards = useMemo(
    () => [
      {
        title: 'Total trajets',
        value: formatInteger(overview?.total_trips),
        icon: '🚆',
        helperText: 'Nombre total de trajets disponibles.',
        accent: 'blue',
        featured: true
      },
      {
        title: 'Distance totale',
        value: `${formatInteger(overview?.total_distance_km)} km`,
        icon: '📏',
        helperText: 'Distance cumulée sur l’ensemble des trajets.',
        accent: 'navy',
        featured: true
      },
      {
        title: 'Routes',
        value: formatInteger(overview?.total_routes),
        icon: '🛤️',
        helperText: 'Nombre d’itinéraires distincts.',
        accent: 'slate'
      },
      {
        title: 'Émissions CO2',
        value: `${formatInteger(overview?.total_emissions_kg)} kg`,
        icon: '🌍',
        helperText: 'Émissions consolidées de la base.',
        accent: 'teal'
      },
      {
        title: 'Distance moyenne',
        value: `${formatDecimal(overview?.avg_distance_km, 1)} km`,
        icon: '📊',
        helperText: 'Distance moyenne par trajet.',
        accent: 'gold'
      },
      {
        title: 'Durée moyenne',
        value: `${formatDecimal(overview?.avg_duration_h, 2)} h`,
        icon: '⏱️',
        helperText: 'Temps moyen observé.',
        accent: 'slate'
      }
    ],
    [overview]
  )

  const spotlight = useMemo(
    () => [
      {
        label: 'Pays dominant',
        value: sortedCountryStats[0]?.country || '—',
        meta: sortedCountryStats[0]
          ? `${formatInteger(sortedCountryStats[0]?.trip_count)} trajets`
          : 'Aucune donnée'
      },
      {
        label: 'Train dominant',
        value: sortedTrainTypeStats[0]?.train_type || '—',
        meta: sortedTrainTypeStats[0]
          ? `${formatInteger(sortedTrainTypeStats[0]?.trip_count)} trajets`
          : 'Aucune donnée'
      },
      {
        label: 'Traction la plus émissive',
        value: sortedTractionStats[0]?.traction || '—',
        meta: sortedTractionStats[0]
          ? `${formatDecimal(sortedTractionStats[0]?.avg_emission_per_km, 3)} kg / km`
          : 'Aucune donnée'
      }
    ],
    [sortedCountryStats, sortedTrainTypeStats, sortedTractionStats]
  )

  if (loading) {
    return (
      <div className="dashboard dashboard-loading">
        <section className="premium-hero premium-skeleton" />
        <section className="stats-grid">
          {Array.from({ length: 6 }).map((_, index) => (
            <div key={index} className="stats-card premium-skeleton" />
          ))}
        </section>
        <section className="charts-grid">
          {Array.from({ length: 3 }).map((_, index) => (
            <div key={index} className="chart-card premium-skeleton" />
          ))}
        </section>
      </div>
    )
  }

  if (error) {
    return (
      <div className="state-card error-state">
        <span className="section-pill">Connexion API</span>
        <h2>Impossible de charger le dashboard</h2>
        <p>{error}</p>
        <button type="button" className="primary-button" onClick={fetchData}>
          Réessayer
        </button>
      </div>
    )
  }

  return (
    <div className="dashboard">
      <section className="premium-hero">
        <div className="premium-hero-main">
          <span className="section-pill">Vue d’ensemble</span>
          <h2>Piloter les trajets ferroviaires en un coup d’œil</h2>
          <p>
            Suivez les volumes, les distances, les durées et les émissions dans une
            interface claire, structurée et orientée décision.
          </p>

          <div className="premium-highlight-band">
            <div className="highlight-item">
              <span>Trajets analysés</span>
              <strong>{formatInteger(overview?.total_trips)}</strong>
            </div>
            <div className="highlight-item">
              <span>Routes distinctes</span>
              <strong>{formatInteger(overview?.total_routes)}</strong>
            </div>
            <div className="highlight-item">
              <span>CO2 total</span>
              <strong>{formatInteger(overview?.total_emissions_kg)} kg</strong>
            </div>
          </div>
        </div>

        <div className="premium-hero-side">
          {spotlight.map((item) => (
            <div className="premium-side-card" key={item.label}>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
              <small>{item.meta}</small>
            </div>
          ))}
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">KPIs</span>
            <h2>Indicateurs clés</h2>
            <p>Les métriques essentielles pour suivre l’activité ferroviaire.</p>
          </div>
        </div>

        <div className="stats-grid premium-stats-grid">
          {cards.map((card) => (
            <StatsCard
              key={card.title}
              title={card.title}
              value={card.value}
              icon={card.icon}
              helperText={card.helperText}
              accent={card.accent}
              featured={card.featured}
            />
          ))}
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">Analyse visuelle</span>
            <h2>Répartitions principales</h2>
            <p>Vue synthétique des pays, matériels roulants et tractions.</p>
          </div>
        </div>

        <div className="charts-grid">
          <ChartCard
            title="Trajets par pays"
            subtitle="Top des pays les plus représentés."
            data={sortedCountryStats.slice(0, 10)}
            dataKey="trip_count"
            nameKey="country"
            axisFormatter={(value) => formatInteger(value)}
            valueFormatter={(value) => `${formatInteger(value)} trajets`}
            barColor="#2c5fdd"
          />

          <ChartCard
            title="Trajets par type de train"
            subtitle="Répartition par matériel roulant."
            data={sortedTrainTypeStats.slice(0, 8)}
            dataKey="trip_count"
            nameKey="train_type"
            axisFormatter={(value) => formatInteger(value)}
            valueFormatter={(value) => `${formatInteger(value)} trajets`}
            barColor="#16324f"
          />

          <ChartCard
            title="Émissions moyennes par traction"
            subtitle="Comparaison des émissions moyennes au km."
            data={sortedTractionStats}
            dataKey="avg_emission_per_km"
            nameKey="traction"
            axisFormatter={(value) => formatDecimal(value, 3)}
            valueFormatter={(value) => `${formatDecimal(value, 3)} kg / km`}
            barColor="#0f9d8a"
            layout="horizontal"
            badgeLabel={`${sortedTractionStats.length} traction${sortedTractionStats.length > 1 ? 's' : ''}`}
          />
        </div>
      </section>

      <section className="dashboard-section">
        <SearchTrips apiUrl={API_URL} />
      </section>
    </div>
  )
}

export default Dashboard