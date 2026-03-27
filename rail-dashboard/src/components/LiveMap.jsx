import React, { useState, useEffect, useMemo } from 'react'
import { MapContainer, TileLayer, CircleMarker, Polyline, Tooltip } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import axios from 'axios'
import './LiveMap.css'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'

// ---------------------------------------------------------------------------
// Dictionnaire de coordonnées des grandes gares européennes (fallback uniquement)
// Clé = fragment du nom de gare (insensible à la casse)
// ---------------------------------------------------------------------------
const STATION_COORDS = {
  // France
  'paris': [48.8566, 2.3522],
  'lyon': [45.7578, 4.8320],
  'marseille': [43.2965, 5.3698],
  'bordeaux': [44.8378, -0.5792],
  'toulouse': [43.6047, 1.4442],
  'nantes': [47.2184, -1.5536],
  'rennes': [48.1173, -1.6778],
  'strasbourg': [48.5734, 7.7521],
  'lille': [50.6292, 3.0573],
  'nice': [43.7102, 7.2620],
  'montpellier': [43.6110, 3.8767],
  'dijon': [47.3220, 5.0415],
  'rouen': [49.4432, 1.0993],
  'le havre': [49.4944, 0.1079],
  'le mans': [47.9960, 0.1966],
  'tours': [47.3941, 0.6848],
  'brest': [48.3904, -4.4861],
  'clermont': [45.7794, 3.0869],
  'grenoble': [45.1885, 5.7245],
  'metz': [49.1193, 6.1757],
  'nancy': [48.6921, 6.1844],
  'reims': [49.2583, 4.0317],
  'perpignan': [42.6986, 2.8956],
  'toulon': [43.1242, 5.9280],

  // Belgique
  'bruxelles': [50.8503, 4.3517],
  'brussels': [50.8503, 4.3517],
  'brugge': [51.2093, 3.2247],
  'bruges': [51.2093, 3.2247],
  'ghent': [51.0543, 3.7174],
  'gand': [51.0543, 3.7174],
  'liège': [50.6326, 5.5797],
  'liege': [50.6326, 5.5797],
  'antwerpen': [51.2194, 4.4025],
  'anvers': [51.2194, 4.4025],

  // Pays-Bas
  'amsterdam': [52.3676, 4.9041],
  'rotterdam': [51.9225, 4.4792],
  'den haag': [52.0705, 4.3007],
  'la haye': [52.0705, 4.3007],
  'utrecht': [52.0907, 5.1214],
  'eindhoven': [51.4416, 5.4697],

  // Allemagne
  'berlin': [52.5200, 13.4050],
  'hamburg': [53.5753, 10.0153],
  'münchen': [48.1351, 11.5820],
  'munich': [48.1351, 11.5820],
  'frankfurt': [50.1109, 8.6821],
  'köln': [50.9333, 6.9500],
  'cologne': [50.9333, 6.9500],
  'stuttgart': [48.7758, 9.1829],
  'düsseldorf': [51.2217, 6.7762],
  'dortmund': [51.5136, 7.4653],
  'hannover': [52.3759, 9.7320],
  'hanover': [52.3759, 9.7320],
  'leipzig': [51.3397, 12.3731],
  'dresden': [51.0504, 13.7373],
  'nürnberg': [49.4521, 11.0767],
  'nuremberg': [49.4521, 11.0767],
  'mannheim': [49.4875, 8.4660],
  'karlsruhe': [49.0069, 8.4037],
  'bremen': [53.0793, 8.8017],
  'essen': [51.4556, 7.0116],
  'dusseldorf': [51.2217, 6.7762],

  // Suisse
  'zürich': [47.3769, 8.5417],
  'zurich': [47.3769, 8.5417],
  'basel': [47.5596, 7.5886],
  'bâle': [47.5596, 7.5886],
  'genève': [46.2044, 6.1432],
  'geneve': [46.2044, 6.1432],
  'geneva': [46.2044, 6.1432],
  'bern': [46.9480, 7.4474],
  'lausanne': [46.5197, 6.6323],

  // Autriche
  'wien': [48.2082, 16.3738],
  'vienne': [48.2082, 16.3738],
  'vienna': [48.2082, 16.3738],
  'salzburg': [47.8095, 13.0550],
  'innsbruck': [47.2692, 11.4041],
  'graz': [47.0707, 15.4395],
  'linz': [48.3069, 14.2858],

  // Italie
  'milano': [45.4654, 9.1859],
  'milan': [45.4654, 9.1859],
  'torino': [45.0703, 7.6869],
  'turin': [45.0703, 7.6869],
  'roma': [41.9028, 12.4964],
  'rome': [41.9028, 12.4964],
  'napoli': [40.8518, 14.2681],
  'naples': [40.8518, 14.2681],
  'firenze': [43.7696, 11.2558],
  'florence': [43.7696, 11.2558],
  'venezia': [45.4408, 12.3155],
  'venice': [45.4408, 12.3155],
  'bologna': [44.4949, 11.3426],
  'genova': [44.4056, 8.9463],
  'genes': [44.4056, 8.9463],
  'verona': [45.4384, 10.9916],
  'bari': [41.1171, 16.8719],
  'palermo': [38.1157, 13.3615],
  'catania': [37.5079, 15.0830],

  // Espagne
  'madrid': [40.4168, -3.7038],
  'barcelona': [41.3851, 2.1734],
  'barcelone': [41.3851, 2.1734],
  'sevilla': [37.3891, -5.9845],
  'séville': [37.3891, -5.9845],
  'zaragoza': [41.6561, -0.8773],
  'valence': [39.4699, -0.3763],
  'valencia': [39.4699, -0.3763],
  'bilbao': [43.2630, -2.9350],
  'málaga': [36.7213, -4.4217],
  'malaga': [36.7213, -4.4217],
  'alicante': [38.3452, -0.4810],
  'salamanca': [40.9701, -5.6635],
  'valladolid': [41.6523, -4.7245],
  'burgos': [42.3440, -3.6969],
  'san sebastian': [43.3183, -1.9812],
  'pamplona': [42.8169, -1.6431],
  'cordoba': [37.8882, -4.7794],
  'granada': [37.1773, -3.5986],

  // Portugal
  'lisboa': [38.7169, -9.1395],
  'lisbonne': [38.7169, -9.1395],
  'lisbon': [38.7169, -9.1395],
  'porto': [41.1579, -8.6291],
  'faro': [37.0194, -7.9322],

  // Royaume-Uni
  'london': [51.5074, -0.1278],
  'londres': [51.5074, -0.1278],
  'manchester': [53.4808, -2.2426],
  'birmingham': [52.4862, -1.8904],
  'liverpool': [53.4084, -2.9916],
  'leeds': [53.8008, -1.5491],
  'glasgow': [55.8642, -4.2518],
  'edinburgh': [55.9533, -3.1883],
  'bristol': [51.4545, -2.5879],
  'cardiff': [51.4816, -3.1791],
  'newcastle': [54.9783, -1.6178],

  // Scandinavie
  'oslo': [59.9139, 10.7522],
  'stockholm': [59.3293, 18.0686],
  'copenhagen': [55.6761, 12.5683],
  'copenhague': [55.6761, 12.5683],
  'kobenhavn': [55.6761, 12.5683],
  'göteborg': [57.7072, 11.9668],
  'gothenburg': [57.7072, 11.9668],
  'helsinki': [60.1699, 24.9384],
  'malmö': [55.6050, 13.0038],
  'malmo': [55.6050, 13.0038],
  'bergen': [60.3913, 5.3221],
  'stavanger': [58.9700, 5.7331],
  'tampere': [61.4978, 23.7610],
  'turku': [60.4518, 22.2666],
  'linkoping': [58.4108, 15.6214],
  'linköping': [58.4108, 15.6214],
  'västerås': [59.6100, 16.5448],
  'vasteras': [59.6100, 16.5448],
  'sundsvall': [62.3908, 17.3069],
  'umeå': [63.8258, 20.2630],
  'umea': [63.8258, 20.2630],

  // Europe de l'Est
  'warszawa': [52.2297, 21.0122],
  'varsovie': [52.2297, 21.0122],
  'warsaw': [52.2297, 21.0122],
  'kraków': [50.0647, 19.9450],
  'cracovie': [50.0647, 19.9450],
  'krakow': [50.0647, 19.9450],
  'gdańsk': [54.3520, 18.6466],
  'gdansk': [54.3520, 18.6466],
  'wrocław': [51.1079, 17.0385],
  'wroclaw': [51.1079, 17.0385],
  'poznań': [52.4064, 16.9252],
  'poznan': [52.4064, 16.9252],
  'katowice': [50.2649, 19.0238],
  'łódź': [51.7592, 19.4560],
  'lodz': [51.7592, 19.4560],
  'Praha': [50.0755, 14.4378],
  'prague': [50.0755, 14.4378],
  'brno': [49.1951, 16.6068],
  'ostrava': [49.8209, 18.2625],
  'bratislava': [48.1486, 17.1077],
  'budapest': [47.4979, 19.0402],
  'wien': [48.2082, 16.3738],
  'zagreb': [45.8150, 15.9819],
  'ljubljana': [46.0569, 14.5058],
  'beograd': [44.7866, 20.4489],
  'belgrade': [44.7866, 20.4489],
  'sarajevo': [43.8476, 18.3564],
  'skopje': [41.9981, 21.4254],
  'tiranë': [41.3275, 19.8187],
  'tirana': [41.3275, 19.8187],
  'sofia': [42.6977, 23.3219],
  'bucharest': [44.4268, 26.1025],
  'bucurești': [44.4268, 26.1025],
  'bucarest': [44.4268, 26.1025],
  'cluj': [46.7712, 23.6236],
  'braşov': [45.6427, 25.5887],
  'brasov': [45.6427, 25.5887],
  'constanta': [44.1598, 28.6348],
  'varna': [43.2141, 27.9147],
  'thessaloniki': [40.6401, 22.9444],
  'salonique': [40.6401, 22.9444],
  'athènes': [37.9838, 23.7275],
  'athens': [37.9838, 23.7275],
  'athina': [37.9838, 23.7275],
  'split': [43.5081, 16.4402],

  // Pays Baltes
  'tallinn': [59.4370, 24.7536],
  'riga': [56.9496, 24.1052],
  'vilnius': [54.6872, 25.2797],

  // Europe orientale / Russie
  'minsk': [53.9045, 27.5615],
  'kiev': [50.4501, 30.5234],
  'kyiv': [50.4501, 30.5234],
  'moscou': [55.7558, 37.6173],
  'moscow': [55.7558, 37.6173],
  'moskva': [55.7558, 37.6173],
  'lwiw': [49.8397, 24.0297],
  'lviv': [49.8397, 24.0297],
  'odessa': [46.4825, 30.7326],
  'smolensk': [54.7826, 32.0453],

  // Turquie
  'istanbul': [41.0082, 28.9784],
  'ankara': [39.9334, 32.8597],
  'izmir': [38.4237, 27.1428],
  'eskişehir': [39.7767, 30.5206],
  'konya': [37.8713, 32.4846],
}

// ---------------------------------------------------------------------------
// Résolution du nom de gare → coordonnées
// ---------------------------------------------------------------------------
function resolveCoords(stopName) {
  if (!stopName) return null
  const name = stopName.toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')

  for (const [key, coords] of Object.entries(STATION_COORDS)) {
    const keyN = key.toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
    if (name === keyN || name.startsWith(keyN) || name.includes(keyN) || keyN.includes(name.split(/[-\s]/)[0])) {
      return coords
    }
  }
  return null
}

// ---------------------------------------------------------------------------
// Plages horaires disponibles
// ---------------------------------------------------------------------------
const HOUR_RANGES = [
  { label: 'Toutes heures', from: null, to: null },
  { label: '0h – 3h',  from: 0,  to: 3  },
  { label: '3h – 6h',  from: 3,  to: 6  },
  { label: '6h – 9h',  from: 6,  to: 9  },
  { label: '9h – 12h', from: 9,  to: 12 },
  { label: '12h – 15h',from: 12, to: 15 },
  { label: '15h – 18h',from: 15, to: 18 },
  { label: '18h – 21h',from: 18, to: 21 },
  { label: '21h – 24h',from: 21, to: 24 },
]

// ---------------------------------------------------------------------------
// Composant principal
// ---------------------------------------------------------------------------
export default function LiveMap() {
  const [shapes, setShapes] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedRange, setSelectedRange] = useState(0) // index dans HOUR_RANGES

  const range = HOUR_RANGES[selectedRange]

  useEffect(() => {
    setLoading(true)
    setError(null)
    const params = {}
    if (range.from !== null) params.hour_from = range.from
    if (range.to   !== null) params.hour_to   = range.to

    axios.get(`${API_BASE}/api/map/shapes`, { params })
      .then(res => {
        setShapes(res.data || [])
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedRange])

  // Construire la liste des segments (tracés GPS réels) et des gares (1er/dernier point)
  const { segments, stations } = useMemo(() => {
    const stationMap = new Map()
    const segs = []

    for (const s of shapes) {
      if (!s.points || s.points.length < 2) continue

      segs.push({
        positions: s.points,          // tableau [[lat,lon], ...] issu de la BDD
        origin: s.origin,
        destination: s.destination,
        departure: s.departure_time,
        arrival: s.arrival_time,
        route: s.route_name,
        agency: s.agency_name,
      })

      // Premier point = gare d'origine, dernier point = gare de destination
      const oCoords = s.points[0]
      const dCoords = s.points[s.points.length - 1]

      if (s.origin && !stationMap.has(s.origin)) {
        stationMap.set(s.origin, { coords: oCoords, name: s.origin })
      }
      if (s.destination && !stationMap.has(s.destination)) {
        stationMap.set(s.destination, { coords: dCoords, name: s.destination })
      }
    }

    return { segments: segs, stations: [...stationMap.values()] }
  }, [shapes])

  const resolvedCount = stations.length
  const totalRoutes = shapes.length

  return (
    <div className="livemap-page">
      {/* En-tête */}
      <div className="livemap-header">
        <div className="livemap-header-copy">
          <span className="section-pill">Trafic en temps réel</span>
          <h2>Trajet en direct</h2>
          <p>
            Visualisez les liaisons ferroviaires européennes sur la carte.
            Chaque tracé suit exactement le parcours GPS réel du train.
          </p>
        </div>
        <div className="livemap-stats-row">
          <div className="livemap-stat-chip">
            <span className="livemap-stat-value">{segments.length}</span>
            <span className="livemap-stat-label">Liaisons affichées</span>
          </div>
          <div className="livemap-stat-chip">
            <span className="livemap-stat-value">{resolvedCount}</span>
            <span className="livemap-stat-label">Gares localisées</span>
          </div>
          <div className="livemap-stat-chip">
            <span className="livemap-stat-value">{totalRoutes}</span>
            <span className="livemap-stat-label">Shapes chargés</span>
          </div>
        </div>
      </div>

      {/* Filtre horaire */}
      <div className="livemap-filter-bar">
        <span className="livemap-filter-label">Filtrer par heure de départ :</span>
        <div className="livemap-filter-pills">
          {HOUR_RANGES.map((hr, i) => (
            <button
              key={i}
              className={`livemap-filter-pill${selectedRange === i ? ' active' : ''}`}
              onClick={() => setSelectedRange(i)}
            >
              {hr.label}
            </button>
          ))}
        </div>
      </div>

      {/* Carte */}
      <div className="livemap-map-wrapper">
        {loading && (
          <div className="livemap-overlay">
            <div className="livemap-spinner" />
            <p>Chargement des données…</p>
          </div>
        )}
        {error && (
          <div className="livemap-overlay livemap-error">
            <p>Erreur de connexion à l'API :<br /><code>{error}</code></p>
            <p style={{ marginTop: 8, fontSize: '0.85rem', opacity: 0.7 }}>Vérifiez que l'API est démarrée sur le port 8000.</p>
          </div>
        )}

        <MapContainer
          center={[50.5, 10]}
          zoom={4}
          minZoom={3}
          maxZoom={10}
          maxBounds={[[34, -25], [72, 45]]}
          maxBoundsViscosity={1.0}
          style={{ height: '100%', width: '100%' }}
          zoomControl={true}
        >
          {/* Fond cartographique CartoDB clair, similaire à l'image de référence */}
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
            subdomains="abcd"
          />

          {/* Lignes rouges = trajets */}
          {!loading && segments.map((seg, i) => (
            <Polyline
              key={i}
              positions={seg.positions}
              pathOptions={{
                color: '#2c5fdd',
                weight: 2,
                opacity: 0.72,
              }}
            >
              <Tooltip sticky>
                <div className="livemap-tooltip">
                  <strong>{seg.origin}</strong>
                  <span className="livemap-tooltip-arrow">→</span>
                  <strong>{seg.destination}</strong>
                  {seg.departure && (
                    <div className="livemap-tooltip-time">
                      {seg.departure} → {seg.arrival}
                    </div>
                  )}
                  {seg.route && <div className="livemap-tooltip-route">{seg.route}</div>}
                  {seg.agency && <div className="livemap-tooltip-agency">{seg.agency}</div>}
                </div>
              </Tooltip>
            </Polyline>
          ))}

          {/* Points bleus = gares */}
          {!loading && stations.map((st, i) => (
            <CircleMarker
              key={i}
              center={st.coords}
              radius={4}
              pathOptions={{
                fillColor: '#16324f',
                fillOpacity: 0.9,
                color: '#ffffff',
                weight: 1.5,
              }}
            >
              <Tooltip direction="top" offset={[0, -6]}>
                <div className="livemap-tooltip">
                  <strong>{st.name}</strong>
                </div>
              </Tooltip>
            </CircleMarker>
          ))}
        </MapContainer>

        {/* Légende */}
        {!loading && !error && (
          <div className="livemap-legend">
            <div className="livemap-legend-item">
              <span className="livemap-legend-dot livemap-legend-station" />
              Gare ferroviaire
            </div>
            <div className="livemap-legend-item">
              <span className="livemap-legend-line" />
              Liaison directe
            </div>
          </div>
        )}
      </div>

      {/* Message si aucune donnée localisée */}
      {!loading && !error && segments.length === 0 && (
        <div className="livemap-empty">
          <p>Aucun trajet localisé pour ce filtre horaire.</p>
          <p style={{ marginTop: 6, fontSize: '0.88rem', opacity: 0.7 }}>
            Aucun shape GPS disponible pour ce filtre horaire.
            {totalRoutes > 0 ? ` (${totalRoutes} shapes chargés, aucun avec assez de points)` : ' Vérifiez la connexion à l\'API ou relancez l\'ETL.'}
          </p>
        </div>
      )}
    </div>
  )
}
