import React from 'react'

const LoadingSkeleton = () => {
  return (
    <div className="dashboard dashboard-loading" data-testid="loading-skeleton">
      <div className="skeleton-header">
        <div className="skeleton skeleton-title" />
        <div className="skeleton skeleton-subtitle" />
      </div>
      
      <div className="skeleton-hero" data-testid="skeleton-hero">
        <div className="skeleton skeleton-large" />
        <div className="skeleton skeleton-medium" />
      </div>
      
      <div className="stats-grid" data-testid="skeleton-stats">
        {Array.from({ length: 6 }).map((_, index) => (
          <div key={index} className="stats-card skeleton-card" data-testid={`skeleton-card-${index}`}>
            <div className="skeleton skeleton-icon" />
            <div className="skeleton skeleton-text" />
            <div className="skeleton skeleton-value" />
          </div>
        ))}
      </div>
      
      <div className="charts-grid" data-testid="skeleton-charts">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="chart-card skeleton-chart" data-testid={`skeleton-chart-${index}`}>
            <div className="skeleton skeleton-title" />
            <div className="skeleton skeleton-chart" />
          </div>
        ))}
      </div>
    </div>
  )
}

export default LoadingSkeleton