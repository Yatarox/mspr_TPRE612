import React from 'react'

function StatsCard({ title, value, icon, helperText, accent = 'blue', featured = false, dataTestId }) {
  return (
    <article 
      className={`stats-card accent-${accent} ${featured ? 'stats-card-featured' : ''}`}
      data-testid={dataTestId || `stats-card-${title.replace(/\s/g, '-').toLowerCase()}`}
    >
      <div className="stats-card-top">
        <div className="stats-card-icon" data-testid="stats-card-icon">{icon}</div>
        <span className="stats-card-pill" data-testid="stats-card-pill">Indicateur</span>
      </div>

      <div className="stats-card-content">
        <p className="stats-card-title" data-testid="stats-card-title">{title}</p>
        <h3 className="stats-card-value" data-testid="stats-card-value">{value}</h3>
        <p className="stats-card-helper" data-testid="stats-card-helper">{helperText}</p>
      </div>

      <div className="stats-card-footer">
        <span className="stats-card-line" data-testid="stats-card-line" />
      </div>
    </article>
  )
}

export default StatsCard