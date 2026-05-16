import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell
} from 'recharts'

const COLORS = ['#2c5fdd', '#16324f', '#c29a54', '#0f9d8a', '#425d78', '#b63d3d', '#244fbb', '#0f7f72']

const CustomTooltip = ({ active, payload, valueFormatter }) => {
  if (!active || !payload || !payload.length) return null
  
  const data = payload[0].payload
  return (
    <div className="custom-tooltip" data-testid="emissions-tooltip">
      <p className="custom-tooltip-label">{data.route_name}</p>
      <p className="custom-tooltip-value">{valueFormatter(data.total_emissions)}</p>
      <p className="custom-tooltip-sub">Opérateur: {data.agency_name}</p>
      <p className="custom-tooltip-sub">Trajets: {data.trip_count} | Moyenne: {data.avg_emission_per_km} g/km</p>
    </div>
  )
}

const TopEmissionsChart = ({ title, subtitle, data, valueFormatter, dataTestId }) => {
  const chartData = data.map((item, index) => ({
    ...item,
    displayName: item.route_name?.length > 20 ? item.route_name.substring(0, 20) + '...' : item.route_name
  }))

  return (
    <article className="chart-card" data-testid={dataTestId || "top-emissions-chart"}>
      <div className="chart-card-header">
        <div>
          <h3 data-testid="emissions-chart-title">{title}</h3>
          {subtitle && <p data-testid="emissions-chart-subtitle">{subtitle}</p>}
        </div>
        <span className="chart-card-badge" data-testid="emissions-chart-badge">
          Top {data.length}
        </span>
      </div>

      {data.length > 0 ? (
        <div className="chart-wrapper" data-testid="emissions-chart-wrapper">
          <ResponsiveContainer width="100%" height={400}>
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ top: 12, right: 30, left: 100, bottom: 8 }}
            >
              <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="rgba(16, 35, 61, 0.10)" />
              <XAxis 
                type="number" 
                tickFormatter={(value) => `${(value / 1000).toFixed(0)}k`}
                tick={{ fontSize: 12, fill: '#6b7c93' }}
              />
              <YAxis 
                type="category" 
                dataKey="displayName" 
                tick={{ fontSize: 12, fill: '#6b7c93' }}
                width={120}
              />
              <Tooltip content={(props) => <CustomTooltip {...props} valueFormatter={valueFormatter} />} />
              <Bar dataKey="total_emissions" radius={[0, 8, 8, 0]}>
                {chartData.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="chart-card-empty" data-testid="emissions-chart-empty">
          <p>Aucune donnée disponible</p>
        </div>
      )}
    </article>
  )
}

export default TopEmissionsChart