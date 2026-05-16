import React, { useState, useEffect } from 'react'
import Dashboard from './components/Dashboard'
import { ThemeProvider } from './context/ThemeContext'

function App() {
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) return null

  return (
    <ThemeProvider>
      <div className="app">
        <div className="app-backdrop" aria-hidden="true" />
        
        <header className="app-header">
          <div className="app-header-card">
            <div className="app-header-copy">
              <span className="eyebrow">
                <span className="eyebrow-dot" />
                Rail Intelligence Platform
              </span>
              <h1>
                Rail Data Warehouse
                <span className="title-badge">v2.0</span>
              </h1>
              <p>
                Visualisez les trajets, les distances, les durées et les émissions
                ferroviaires dans un tableau de bord clair et structuré.
              </p>
              <div className="header-stats-mini">
                <div className="mini-stat">
                  <span className="mini-stat-label">Mise à jour</span>
                  <span className="mini-stat-value">En temps réel</span>
                </div>
                <div className="mini-stat">
                  <span className="mini-stat-label">Qualité données</span>
                  <span className="mini-stat-value">99.9%</span>
                </div>
              </div>
            </div>

            <div className="app-header-side">
              <div className="header-side-card">
                <div className="header-side-icon">🎯</div>
                <div>
                  <span>Environnement</span>
                  <strong>Dashboard analytique</strong>
                </div>
              </div>

              <div className="header-side-card">
                <div className="header-side-icon">📡</div>
                <div>
                  <span>Source</span>
                  <strong>Données GTFS + API</strong>
                </div>
              </div>

              <div className="header-side-card">
                <div className="header-side-icon">⚡</div>
                <div>
                  <span>Performance</span>
                  <strong>&lt; 200ms réponse</strong>
                </div>
              </div>
            </div>
          </div>
        </header>

        <main className="app-content">
          <Dashboard />
        </main>
      </div>
    </ThemeProvider>
  )
}

export default App