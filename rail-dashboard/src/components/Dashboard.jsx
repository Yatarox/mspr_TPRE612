import React, { useEffect, useMemo, useState, useCallback } from 'react'
import axios from 'axios'
import StatsCard from './StatsCard'
import ChartCard from './ChartCard'
import SearchTrips from './SearchTrips'
import DateRangePicker from './DateRangePicker'
import ExportButton from './ExportButton'
import Toast from './Toast'
import LoadingSkeleton from './LoadingSkeleton'
import EmissionsPredictor from './EmissionsPredictor'
import TopEmissionsChart from './TopEmissionsChart'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const formatInteger = (value) =>
  new Intl.NumberFormat('fr-FR').format(Number(value) || 0)

const formatDecimal = (value, digits = 1) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits
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
  const [serviceTypeStats, setServiceTypeStats] = useState([])
  const [agencyStats, setAgencyStats] = useState([])
  const [emissionsByRoute, setEmissionsByRoute] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [toast, setToast] = useState(null)
  const [refreshKey, setRefreshKey] = useState(0)

  const showToast = useCallback((message, type = 'info') => {
    setToast({ message, type })
    setTimeout(() => setToast(null), 5000)
  }, [])

  const fetchData = useCallback(async () => {
    try {
      setLoading(true)
      setError('')

      const [
        overviewRes,
        countryRes,
        trainTypeRes,
        tractionRes,
        serviceTypeRes,
        agencyRes,
        emissionsRes
      ] = await Promise.allSettled([
        axios.get(`${API_URL}/api/stats/overview`),
        axios.get(`${API_URL}/api/stats/by-country`),
        axios.get(`${API_URL}/api/stats/by-train-type`),
        axios.get(`${API_URL}/api/stats/by-traction`),
        axios.get(`${API_URL}/api/stats/by-service-type`),
        axios.get(`${API_URL}/api/stats/by-agency`, { params: { limit: 10 } }),
        axios.get(`${API_URL}/api/emissions/by-route`, { params: { limit: 10 } })
      ])

      if (overviewRes.status === 'fulfilled') setOverview(overviewRes.value.data)
      if (countryRes.status === 'fulfilled') setCountryStats(Array.isArray(countryRes.value.data) ? countryRes.value.data : [])
      if (trainTypeRes.status === 'fulfilled') setTrainTypeStats(Array.isArray(trainTypeRes.value.data) ? trainTypeRes.value.data : [])
      if (tractionRes.status === 'fulfilled') setTractionStats(Array.isArray(tractionRes.value.data) ? tractionRes.value.data : [])
      if (serviceTypeRes.status === 'fulfilled') setServiceTypeStats(Array.isArray(serviceTypeRes.value.data) ? serviceTypeRes.value.data : [])
      if (agencyRes.status === 'fulfilled') setAgencyStats(Array.isArray(agencyRes.value.data) ? agencyRes.value.data : [])
      if (emissionsRes.status === 'fulfilled') setEmissionsByRoute(Array.isArray(emissionsRes.value.data) ? emissionsRes.value.data : [])

      if (overviewRes.status === 'rejected') throw overviewRes.reason
      
      showToast('Données mises à jour avec succès', 'success')
    } catch (err) {
      setError(`Erreur de connexion à l’API : ${getErrorMessage(err)}`)
      showToast(getErrorMessage(err), 'error')
      console.error(err)
    } finally {
      setLoading(false)
    }
  }, [showToast])

  useEffect(() => {
    fetchData()
  }, [fetchData, refreshKey])

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

  const sortedServiceTypeStats = useMemo(
    () => [...serviceTypeStats].sort((a, b) => (b?.trip_count ?? 0) - (a?.trip_count ?? 0)),
    [serviceTypeStats]
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
        title: 'Routes distinctes',
        value: formatInteger(overview?.total_routes),
        icon: '🛤️',
        helperText: 'Nombre d’itinéraires distincts.',
        accent: 'slate'
      },
      {
        title: 'Agences',
        value: formatInteger(overview?.total_agencies),
        icon: '🏢',
        helperText: 'Nombre d’opérateurs ferroviaires.',
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
      },
      {
        title: 'Émission moyenne',
        value: `${formatDecimal(overview?.avg_emission_per_km, 2)} g CO₂/km`,
        icon: '💨',
        helperText: 'Émissions moyennes par km.',
        accent: 'teal'
      }
    ],
    [overview]
  )

  const spotlight = useMemo(
    () => [
      {
        label: 'Pays dominant',
        value: sortedCountryStats[0]?.country_name || sortedCountryStats[0]?.country || '—',
        icon: '🌍',
        meta: sortedCountryStats[0]
          ? `${formatInteger(sortedCountryStats[0]?.trip_count)} trajets | ${formatDecimal(sortedCountryStats[0]?.avg_emission_per_km, 2)} g CO₂/km`
          : 'Aucune donnée'
      },
      {
        label: 'Train dominant',
        value: sortedTrainTypeStats[0]?.train_type || '—',
        icon: '🚄',
        meta: sortedTrainTypeStats[0]
          ? `${formatInteger(sortedTrainTypeStats[0]?.trip_count)} trajets | ${formatDecimal(sortedTrainTypeStats[0]?.avg_emission_per_km, 2)} g CO₂/km`
          : 'Aucune donnée'
      },
      {
        label: 'Traction la plus émissive',
        value: sortedTractionStats[0]?.traction || '—',
        icon: '⚡',
        meta: sortedTractionStats[0]
          ? `${formatDecimal(sortedTractionStats[0]?.avg_emission_per_km, 2)} g CO₂/km`
          : 'Aucune donnée'
      }
    ],
    [sortedCountryStats, sortedTrainTypeStats, sortedTractionStats]
  )

  const handleRefresh = () => {
    setRefreshKey(prev => prev + 1)
  }

  if (loading && !overview) {
    return (
      <div data-testid="dashboard-loading">
        <LoadingSkeleton />
      </div>
    )
  }

  if (error && !overview) {
    return (
      <div className="state-card error-state" data-testid="dashboard-error">
        <span className="section-pill">⚠️ Connexion API</span>
        <h2>Impossible de charger le dashboard</h2>
        <p>{error}</p>
        <div className="error-actions">
          <button type="button" className="primary-button" onClick={fetchData} data-testid="retry-button">
            🔄 Réessayer
          </button>
          <button type="button" className="secondary-button" onClick={() => window.location.reload()} data-testid="reload-button">
            🔁 Recharger la page
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="dashboard" data-testid="dashboard-content">
      {toast && <Toast message={toast.message} type={toast.type} />}

      <div className="dashboard-header-actions" data-testid="dashboard-header-actions">
        <div className="header-info">
          <span className="live-badge" data-testid="live-badge">● Live</span>
          <span className="update-info" data-testid="update-info">Dernière mise à jour: {new Date().toLocaleString('fr-FR')}</span>
        </div>
        <div className="header-actions">
          <ExportButton 
            data={{ overview, countryStats, trainTypeStats, tractionStats, agencyStats, emissionsByRoute }}
            fileName="rail_dashboard_export"
          />
          <button className="icon-button" onClick={handleRefresh} title="Actualiser" data-testid="refresh-button">
            🔄
          </button>
        </div>
      </div>

      <section className="premium-hero" data-testid="premium-hero">
        <div className="premium-hero-main">
          <span className="section-pill">
            <span className="pill-icon">📈</span>
            Vue d’ensemble
          </span>
          <h2>Performance et impact environnemental du réseau ferroviaire</h2>
          <p>
            Analysez les indicateurs clés du réseau : volumes de trafic, efficacité opérationnelle
            et empreinte carbone détaillée par type de train, traction et région.
          </p>

          <div className="premium-highlight-band" data-testid="highlight-band">
            <div className="highlight-item" data-testid="highlight-total-trips">
              <span>Trajets analysés</span>
              <strong>{formatInteger(overview?.total_trips)}</strong>
              <div className="highlight-meta">{formatInteger(overview?.total_routes)} routes</div>
            </div>
            <div className="highlight-item" data-testid="highlight-total-distance">
              <span>Distance totale</span>
              <strong>{formatInteger(overview?.total_distance_km)} km</strong>
              <div className="highlight-meta">~{formatInteger((overview?.total_distance_km || 0) / 40075)} tours de Terre</div>
            </div>
            <div className="highlight-item" data-testid="highlight-total-emissions">
              <span>CO₂ total</span>
              <strong>{formatInteger(overview?.total_emissions_kg)} kg</strong>
              <div className="highlight-meta">{formatDecimal(overview?.avg_emission_per_km, 2)} g/km en moyenne</div>
            </div>
          </div>
        </div>

        <div className="premium-hero-side" data-testid="premium-hero-side">
          {spotlight.map((item, index) => (
            <div className="premium-side-card" key={item.label} data-testid={`spotlight-${index}`}>
              <div className="side-card-icon">{item.icon}</div>
              <div>
                <span>{item.label}</span>
                <strong>{item.value}</strong>
                <small>{item.meta}</small>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="dashboard-section" data-testid="kpi-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">
              <span className="pill-icon">🎯</span>
              KPIs
            </span>
            <h2>Indicateurs clés de performance</h2>
            <p>Les métriques essentielles pour suivre l’activité ferroviaire et son impact environnemental.</p>
          </div>
        </div>

        <div className="stats-grid premium-stats-grid" data-testid="kpi-grid">
          {cards.map((card, index) => (
            <StatsCard
              key={card.title}
              title={card.title}
              value={card.value}
              icon={card.icon}
              helperText={card.helperText}
              accent={card.accent}
              featured={card.featured}
              data-testid={`kpi-card-${index}`}
            />
          ))}
        </div>
      </section>

      <section className="dashboard-section" data-testid="charts-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">
              <span className="pill-icon">📊</span>
              Analyse détaillée
            </span>
            <h2>Répartitions et comparaisons</h2>
            <p>Vue synthétique des pays, matériels roulants, tractions et services.</p>
          </div>
        </div>

        <div className="charts-grid" data-testid="charts-grid">
          <div data-testid="chart-country">
            <ChartCard
              title="Trajets par pays"
              subtitle="Top des pays d'origine les plus représentés."
              data={sortedCountryStats.slice(0, 10)}
              dataKey="trip_count"
              nameKey="country_name"
              axisFormatter={(value) => formatInteger(value)}
              valueFormatter={(value) => `${formatInteger(value)} trajets`}
              barColor="#2c5fdd"
              animation={true}
            />
          </div>

          <div data-testid="chart-train-type">
            <ChartCard
              title="Trajets par type de train"
              subtitle="Répartition par matériel roulant."
              data={sortedTrainTypeStats.slice(0, 8)}
              dataKey="trip_count"
              nameKey="train_type"
              axisFormatter={(value) => formatInteger(value)}
              valueFormatter={(value) => `${formatInteger(value)} trajets`}
              barColor="#16324f"
              animation={true}
            />
          </div>

          <div data-testid="chart-service-type">
            <ChartCard
              title="Répartition jour / nuit"
              subtitle="Comparaison des trajets selon le service de jour ou de nuit."
              data={sortedServiceTypeStats}
              dataKey="trip_count"
              nameKey="service_type"
              valueFormatter={(value) => `${formatInteger(value)} trajets`}
              variant="pie"
              badgeLabel={`${sortedServiceTypeStats.length} type${sortedServiceTypeStats.length > 1 ? 's' : ''}`}
              animation={true}
            />
          </div>

          <div data-testid="chart-traction">
            <ChartCard
              title="Émissions moyennes par traction"
              subtitle="Comparaison des émissions moyennes au km par type de traction."
              data={sortedTractionStats}
              dataKey="avg_emission_per_km"
              nameKey="traction"
              axisFormatter={(value) => formatDecimal(value, 2)}
              valueFormatter={(value) => `${formatDecimal(value, 2)} g CO₂/km`}
              barColor="#0f9d8a"
              layout="horizontal"
              badgeLabel={`${sortedTractionStats.length} traction${sortedTractionStats.length > 1 ? 's' : ''}`}
              animation={true}
            />
          </div>
        </div>
      </section>

      <section className="dashboard-section" data-testid="operators-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">
              <span className="pill-icon">🏢</span>
              Opérateurs & Routes
            </span>
            <h2>Top opérateurs et routes émettrices</h2>
            <p>Classement des agences et itinéraires les plus impactants.</p>
          </div>
        </div>

        <div className="two-columns-grid" data-testid="two-columns-grid">
          {agencyStats.length > 0 && (
            <div data-testid="chart-agency">
              <ChartCard
                title="Top 10 des opérateurs"
                subtitle="Agences avec le plus de trajets."
                data={agencyStats}
                dataKey="trip_count"
                nameKey="agency_name"
                axisFormatter={(value) => formatInteger(value)}
                valueFormatter={(value) => `${formatInteger(value)} trajets`}
                barColor="#c29a54"
                layout="horizontal"
                animation={true}
              />
            </div>
          )}

          {emissionsByRoute.length > 0 && (
            <div data-testid="chart-emissions-route">
              <TopEmissionsChart
                title="Routes les plus émettrices"
                subtitle="Classement par émissions totales de CO₂."
                data={emissionsByRoute.slice(0, 8)}
                valueFormatter={(value) => `${formatInteger(value)} kg CO₂`}
              />
            </div>
          )}
        </div>
      </section>

      <section className="dashboard-section" data-testid="predictor-section">
        <div className="section-heading">
          <div>
            <span className="section-pill">
              <span className="pill-icon">🤖</span>
              Intelligence Artificielle
            </span>
            <h2>Prédiction des émissions CO₂</h2>
            <p>Estimez l'impact carbone d'un trajet grâce à notre modèle de Machine Learning.</p>
          </div>
        </div>
        <EmissionsPredictor apiUrl={API_URL} />
      </section>

      <section className="dashboard-section" data-testid="search-section">
        <SearchTrips apiUrl={API_URL} />
      </section>
    </div>
  )
}

export default Dashboard