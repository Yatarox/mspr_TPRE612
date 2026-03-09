import React from 'react'

function StatsCard({ title, value, icon, helperText, accent = 'blue', featured = false }) {
  return (
    <article className={`stats-card accent-${accent} ${featured ? 'stats-card-featured' : ''}`}>
      <div className="stats-card-top">
        <div className="stats-card-icon">{icon}</div>
        <span className="stats-card-pill">Indicateur</span>
      </div>

      <div className="stats-card-content">
        <p className="stats-card-title">{title}</p>
        <h3 className="stats-card-value">{value}</h3>
        <p className="stats-card-helper">{helperText}</p>
      </div>

      <div className="stats-card-footer">
        <span className="stats-card-line" />
      </div>
    </article>
  )
}

export default StatsCard