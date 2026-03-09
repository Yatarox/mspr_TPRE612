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

  const summaryCards = useMemo(
    () => [
      {
        title: 'Total trajets',
        value: formatInteger(overview?.total_trips),
        icon: '🚆',
        helperText: 'Volume global des trajets enregistrés.'
      },
      {
        title: 'Routes',
        value: formatInteger(overview?.total_routes),
        icon: '🛤️',
        helperText: 'Nombre d’itinéraires distincts disponibles.'
      },
      {
        title: 'Distance totale',
        value: `${formatInteger(overview?.total_distance_km)} km`,
        icon: '📏',
        helperText: 'Distance cumulée sur l’ensemble des trajets.'
      },
      {
        title: 'Émissions CO2',
        value: `${formatInteger(overview?.total_emissions_kg)} kg`,
        icon: '🌍',
        helperText: 'Émissions consolidées sur la base analysée.'
      },
      {
        title: 'Distance moyenne',
        value: `${formatDecimal(overview?.avg_distance_km, 1)} km`,
        icon: '📊',
        helperText: 'Distance moyenne observée par trajet.'
      },
      {
        title: 'Durée moyenne',
        value: `${formatDecimal(overview?.avg_duration_h, 2)} h`,
        icon: '⏱️',
        helperText: 'Temps moyen constaté pour un trajet.'
      }
    ],
    [overview]
  )

  const heroMetrics = useMemo(
    () => [
      {
        label: 'Trajets enregistrés',
        value: formatInteger(overview?.total_trips),
        meta: 'Base consolidée'
      },
      {
        label: 'Pays le plus représenté',
        value: sortedCountryStats[0]?.country || '—',
        meta: sortedCountryStats[0]
          ? `${formatInteger(sortedCountryStats[0]?.trip_count)} trajets`
          : 'Pas de donnée'
      },
      {
        label: 'Type de train dominant',
        value: sortedTrainTypeStats[0]?.train_type || '—',
        meta: sortedTrainTypeStats[0]
          ? `${formatInteger(sortedTrainTypeStats[0]?.trip_count)} trajets`
          : 'Pas de donnée'
      },
      {
        label: 'CO2 total',
        value: `${formatInteger(overview?.total_emissions_kg)} kg`,
        meta: 'Vision environnementale'
      }
    ],
    [overview, sortedCountryStats, sortedTrainTypeStats]
  )

  if (loading) {
    return <LoadingDashboard />
  }

  if (error) {
    return (
      <div className="state-card error-state">
        <span className="section-badge">Erreur</span>
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
      <section className="hero-panel">
        <div className="hero-copy">
          <span className="section-badge">Vue d’ensemble</span>
          <h2>Un dashboard lisible, sobre et prêt pour la restitution</h2>
          <p>
            L’essentiel des données ferroviaires est regroupé ici : indicateurs clés,
            répartitions principales et recherche ciblée pour retrouver rapidement les
            trajets utiles pendant la démo.
          </p>
        </div>

        <div className="hero-metrics">
          {heroMetrics.map((metric) => (
            <div className="hero-metric" key={metric.label}>
              <span className="hero-metric-label">{metric.label}</span>
              <strong className="hero-metric-value">{metric.value}</strong>
              <span className="hero-metric-meta">{metric.meta}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <h2>Indicateurs clés</h2>
            <p>Les métriques à surveiller en priorité pour une lecture immédiate.</p>
          </div>
        </div>

        <div className="stats-grid">
          {summaryCards.map((card) => (
            <StatsCard
              key={card.title}
              title={card.title}
              value={card.value}
              icon={card.icon}
              helperText={card.helperText}
            />
          ))}
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <h2>Répartition des données</h2>
            <p>Une vue simple des volumes, catégories et niveaux d’émission.</p>
          </div>
        </div>

        <div className="charts-grid">
          <ChartCard
            title="Trajets par pays"
            subtitle="Top 10 des pays les plus représentés."
            data={sortedCountryStats.slice(0, 10)}
            dataKey="trip_count"
            nameKey="country"
            axisFormatter={(value) => formatInteger(value)}
            valueFormatter={(value) => `${formatInteger(value)} trajets`}
          />

          <ChartCard
            title="Trajets par type de train"
            subtitle="Répartition des trajets selon le matériel utilisé."
            data={sortedTrainTypeStats.slice(0, 8)}
            dataKey="trip_count"
            nameKey="train_type"
            axisFormatter={(value) => formatInteger(value)}
            valueFormatter={(value) => `${formatInteger(value)} trajets`}
          />

          <ChartCard
            title="Émissions moyennes par traction"
            subtitle="Lecture de l’impact moyen au kilomètre selon la traction."
            data={sortedTractionStats}
            dataKey="avg_emission_per_km"
            nameKey="traction"
            axisFormatter={(value) => formatDecimal(value, 3)}
            valueFormatter={(value) => `${formatDecimal(value, 3)} kg / km`}
          />
        </div>
      </section>

      <section className="dashboard-section">
        <SearchTrips apiUrl={API_URL} />
      </section>
    </div>
  )
}

function LoadingDashboard() {
  return (
    <div className="dashboard dashboard-loading">
      <section className="hero-panel skeleton" aria-hidden="true" />

      <section className="stats-grid" aria-hidden="true">
        {Array.from({ length: 6 }).map((_, index) => (
          <div key={index} className="stats-card skeleton" />
        ))}
      </section>

      <section className="charts-grid" aria-hidden="true">
        {Array.from({ length: 3 }).map((_, index) => (
          <div key={index} className="chart-card skeleton" />
        ))}
      </section>
    </div>
  )
}

export default Dashboard