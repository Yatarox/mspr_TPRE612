import React from 'react'
import Dashboard from './components/Dashboard'

function App() {
  return (
    <div className="app">
      <div className="app-background" aria-hidden="true" />

      <header className="app-header">
        <div className="header-badge">Dashboard analytique</div>

        <div className="header-main">
          <div>
            <h1>Rail Data Warehouse</h1>
            <p>
              Analyse claire des trajets ferroviaires, des distances, des émissions
              et de la recherche opérationnelle sur les données GTFS.
            </p>
          </div>

          <div className="header-chip">Vue globale du réseau</div>
        </div>
      </header>

      <main className="app-content">
        <Dashboard />
      </main>
    </div>
  )
}

export default App