import React from 'react'

function StatsCard({ title, value, icon, helperText }) {
  return (
    <article className="stats-card">
      <div className="stats-card-top">
        <span className="stats-card-icon" aria-hidden="true">
          {icon}
        </span>
        <span className="stats-card-tag">KPI</span>
      </div>

      <div className="stats-card-body">
        <p className="stats-card-title">{title}</p>
        <p className="stats-card-value">{value}</p>
        {helperText ? <p className="stats-card-helper">{helperText}</p> : null}
      </div>
    </article>
  )
}

export default StatsCard