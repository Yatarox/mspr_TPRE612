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
  return label.length > 14 ? `${label.slice(0, 14)}…` : label
}

function ChartCard({
  title,
  subtitle,
  data = [],
  dataKey,
  nameKey,
  valueFormatter = defaultValueFormatter,
  axisFormatter = defaultValueFormatter
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
        <div className="chart-card-title-group">
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>

      {data.length > 0 ? (
        <div className="chart-wrapper">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={data} margin={{ top: 10, right: 16, left: -8, bottom: 14 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                vertical={false}
                stroke="rgba(148, 163, 184, 0.22)"
              />
              <XAxis
                dataKey={nameKey}
                tickFormatter={truncateLabel}
                tick={{ fontSize: 12, fill: '#64748b' }}
                tickLine={false}
                axisLine={false}
                interval={0}
                angle={-25}
                textAnchor="end"
                height={64}
              />
              <YAxis
                tickFormatter={(value) => axisFormatter(value)}
                tick={{ fontSize: 12, fill: '#64748b' }}
                tickLine={false}
                axisLine={false}
                width={72}
              />
              <Tooltip
                content={renderTooltip}
                cursor={{ fill: 'rgba(37, 99, 235, 0.08)' }}
              />
              <Bar
                dataKey={dataKey}
                radius={[10, 10, 0, 0]}
                fill="#2563eb"
                maxBarSize={42}
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