# Frontend Microservice - Rail Dashboard

## Overview

Interface web React pour visualiser les données du projet **Rail Data Warehouse**.  
Le dashboard consomme l’API backend et affiche :
- KPIs globaux
- Graphiques de synthèse
- Recherche de trajets

> ⚠️ Cette partie est prévue pour évoluer (UI, composants, endpoints).

---

## Stack

- React
- Vite
- CSS (fichier `App.css`)
- Consommation API REST (`/api/...`)

---

## Structure actuelle

```text
rail-dashboard/
├── src/
│   ├── App.jsx                 # Layout principal (header + Dashboard)
│   ├── main.jsx                # Point d’entrée React
│   ├── App.css                 # Styles globaux
│   └── components/
│       └── Dashboard.jsx       # Vue principale du dashboard
├── Dockerfile
├── package.json
└── README.md
```

---

## Fichiers principaux

### `src/main.jsx`
- Monte l’application React dans `#root`
- Active `React.StrictMode`

### `src/App.jsx`
- Structure globale de la page
- Affiche le titre + sous-titre
- Charge le composant `Dashboard`

### `src/App.css`
- Style global de l’application
- Responsive basique
- Styles des cartes, formulaires, tableaux, états loading/erreur

---

## Lancement local (Windows)

```bash
npm install
npm run dev
```

Frontend disponible sur l’URL affichée par Vite (souvent `http://localhost:5173`).

---

## Build production

```bash
npm run build
npm run preview
```

---

## API backend

Le frontend attend une API backend active (microservice `api`) pour récupérer :
- stats globales
- stats par dimensions
- recherche de trajets

Si besoin, configurer l’URL API via variable d’environnement Vite (ex: `VITE_API_BASE_URL`).

---

## Notes

- README volontairement léger car le front sera modifié.
- La structure composants/routes pourra être refactorée ensuite.