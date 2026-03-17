import React from 'react'
import Dashboard from './components/Dashboard'

function App() {
  return (
    <div className="app">
      <div className="app-backdrop" aria-hidden="true" />

      <header className="app-header">
        <div className="app-header-card">
          <div className="app-header-copy">
            <span className="eyebrow">Rail Intelligence Platform</span>
            <h1>Rail Data Warehouse</h1>
            <p>
              Visualisez les trajets, les distances, les durées et les émissions
              ferroviaires dans un tableau de bord clair et structuré.
            </p>
          </div>

          <div className="app-header-side">
            <div className="header-side-card">
              <span>Environnement</span>
              <strong>Dashboard analytique</strong>
            </div>

            <div className="header-side-card">
              <span>Source</span>
              <strong>Données GTFS + API</strong>
            </div>

            <div className="header-side-card">
              <span>Usage</span>
              <strong>Exploration & restitution</strong>
            </div>
          </div>
        </div>
      </header>

      <main className="app-content">
        <Dashboard />
      </main>
    </div>
  )
}

export default App