import React from 'react'

function StatsCard({ title, value, icon }) {
  return (
    <div className="stats-card">
      <h3>{title}</h3>
      <div className="value">{icon} {value}</div>
    </div>
  )
}

export default StatsCard