import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts'

const defaultValueFormatter = (value) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: 2
  }).format(Number(value) || 0)

const truncateLabel = (value) => {
  const label = String(value ?? '')
  return label.length > 16 ? `${label.slice(0, 16)}…` : label
}

function ChartCard({
  title,
  subtitle,
  data = [],
  dataKey,
  nameKey,
  valueFormatter = defaultValueFormatter,
  axisFormatter = defaultValueFormatter,
  barColor = '#2c5fdd'
}) {
  const renderTooltip = ({ active, payload, label }) => {
    if (!active || !payload || !payload.length) return null

    return (
      <div className="custom-tooltip">
        <p className="custom-tooltip-label">{label}</p>
        <p className="custom-tooltip-value">{valueFormatter(payload[0].value)}</p>
      </div>
    )
  }

  return (
    <article className="chart-card">
      <div className="chart-card-header">
        <div>
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        <span className="chart-card-badge">{data.length} catégories</span>
      </div>

      {data.length > 0 ? (
        <div className="chart-wrapper">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={data} margin={{ top: 12, right: 12, left: -8, bottom: 16 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                vertical={false}
                stroke="rgba(16, 35, 61, 0.10)"
              />
              <XAxis
                dataKey={nameKey}
                tickFormatter={truncateLabel}
                tick={{ fontSize: 12, fill: '#6b7c93' }}
                tickLine={false}
                axisLine={false}
                interval={0}
                angle={-22}
                textAnchor="end"
                height={62}
              />
              <YAxis
                tickFormatter={(value) => axisFormatter(value)}
                tick={{ fontSize: 12, fill: '#6b7c93' }}
                tickLine={false}
                axisLine={false}
                width={72}
              />
              <Tooltip
                content={renderTooltip}
                cursor={{ fill: 'rgba(44, 95, 221, 0.08)' }}
              />
              <Bar
                dataKey={dataKey}
                fill={barColor}
                radius={[10, 10, 0, 0]}
                maxBarSize={40}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="chart-card-empty">
          <p>Aucune donnée disponible pour ce graphique.</p>
        </div>
      )}
    </article>
  )
}

export default ChartCard