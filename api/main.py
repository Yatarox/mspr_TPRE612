# --- 1. LES OUTILS (On prépare notre cuisine) ---
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# --- 2. LE CHEMIN VERS LE FRIGO (La connexion à la base de données) ---
URL_BASE_DE_DONNEES = "mysql+pymysql://root:root@mysql:3306/rail_dw"

moteur = create_engine(URL_BASE_DE_DONNEES)
SessionLocale = sessionmaker(autocommit=False, autoflush=False, bind=moteur)
Base = declarative_base()

# --- 3. LE PLAN DE LA BOÎTE (Comment est rangé un trajet dans le frigo) ---
class Trajet(Base):
    __tablename__ = "v_api_trips" 

    fact_id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(String)
    route_name = Column(String)
    agency_name = Column(String)
    service_type = Column(String)
    origin = Column(String)
    destination = Column(String)
    departure_time = Column(String)
    arrival_time = Column(String)
    distance_km = Column(Float)
    duration_h = Column(Float)

# --- 4. LE RESTAURANT (Création de l'API) ---
app = FastAPI(title="API ObRail Europe")

def obtenir_base_de_donnees():
    db = SessionLocale()
    try:
        yield db
    finally:
        db.close()

# --- 5. LE GUICHETIER (La route pour poser des questions) ---
@app.get("/api/trajets")
def lire_les_trajets(
    origin: str = None, 
    destination: str = None, 
    db: Session = Depends(obtenir_base_de_donnees)
):
    recherche = db.query(Trajet)
    
    if origin:
        recherche = recherche.filter(Trajet.origin == origin)
        
    if destination:
        recherche = recherche.filter(Trajet.destination == destination)
        
    return recherche.limit(100).all()