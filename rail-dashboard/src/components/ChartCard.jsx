import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend
} from 'recharts'

const defaultValueFormatter = (value) =>
  new Intl.NumberFormat('fr-FR', {
    maximumFractionDigits: 2
  }).format(Number(value) || 0)

const truncateLabel = (value) => {
  const label = String(value ?? '')
  return label.length > 16 ? `${label.slice(0, 16)}…` : label
}

const PIE_COLORS = ['#2c5fdd', '#16324f', '#c29a54', '#0f9d8a', '#94a3b8']

function ChartCard({
  title,
  subtitle,
  data = [],
  dataKey,
  nameKey,
  valueFormatter = defaultValueFormatter,
  axisFormatter = defaultValueFormatter,
  barColor = '#2c5fdd',
  layout = 'vertical',
  badgeLabel,
  variant = 'bar'
}) {
  const isHorizontal = layout === 'horizontal'
  const isPie = variant === 'pie'

  const renderTooltip = ({ active, payload, label }) => {
    if (!active || !payload || !payload.length) return null

    return (
      <div className="custom-tooltip">
        <p className="custom-tooltip-label">{label || payload?.[0]?.name}</p>
        <p className="custom-tooltip-value">{valueFormatter(payload[0].value)}</p>
      </div>
    )
  }

  return (
    <article className={`chart-card ${isHorizontal ? 'chart-card-horizontal' : ''}`}>
      <div className="chart-card-header">
        <div>
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        <span className="chart-card-badge">
          {badgeLabel || `${data.length} catégorie${data.length > 1 ? 's' : ''}`}
        </span>
      </div>

      {data.length > 0 ? (
        <div className="chart-wrapper">
          {isPie ? (
            <ResponsiveContainer width="100%" height={320}>
              <PieChart>
                <Pie
                  data={data}
                  dataKey={dataKey}
                  nameKey={nameKey}
                  cx="50%"
                  cy="50%"
                  outerRadius={105}
                  label
                >
                  {data.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={renderTooltip} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <ResponsiveContainer width="100%" height={isHorizontal ? 360 : 320}>
              <BarChart
                data={data}
                layout={isHorizontal ? 'vertical' : 'horizontal'}
                margin={
                  isHorizontal
                    ? { top: 12, right: 18, left: 24, bottom: 8 }
                    : { top: 12, right: 12, left: -8, bottom: 16 }
                }
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  vertical={!isHorizontal}
                  horizontal={isHorizontal}
                  stroke="rgba(16, 35, 61, 0.10)"
                />

                {isHorizontal ? (
                  <>
                    <XAxis
                      type="number"
                      tickFormatter={(value) => axisFormatter(value)}
                      tick={{ fontSize: 12, fill: '#6b7c93' }}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      type="category"
                      dataKey={nameKey}
                      tickFormatter={truncateLabel}
                      tick={{ fontSize: 12, fill: '#6b7c93' }}
                      tickLine={false}
                      axisLine={false}
                      width={120}
                    />
                  </>
                ) : (
                  <>
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
                  </>
                )}

                <Tooltip content={renderTooltip} cursor={{ fill: 'rgba(44, 95, 221, 0.08)' }} />

                <Bar
                  dataKey={dataKey}
                  fill={barColor}
                  radius={isHorizontal ? [0, 10, 10, 0] : [10, 10, 0, 0]}
                  maxBarSize={isHorizontal ? 24 : 40}
                />
              </BarChart>
            </ResponsiveContainer>
          )}
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