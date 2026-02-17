import React from 'react'
import Dashboard from './components/Dashboard'

function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1>🚂 Rail Data Warehouse</h1>
        <p>Analyse des données ferroviaires GTFS</p>
      </header>
      <Dashboard />
    </div>
  )
}

export default App