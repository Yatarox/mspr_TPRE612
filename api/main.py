from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
# Ici on importerait SessionLocal (ton fichier de connexion) et le modèle Trip (ton fichier model)

app = FastAPI(title="API ObRail Europe")

# Fonction pour ouvrir et fermer la connexion à chaque requête
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Création de la route avec nos filtres dans le query string
@app.get("/api/trajets")
def get_trajets(
    origin: str = None, # Paramètre optionnel dans l'URL (?origin=...)
    destination: str = None, # Paramètre optionnel dans l'URL (?destination=...)
    db: Session = Depends(get_db)
):
    # On prépare la requête de base (SELECT * FROM v_api_trips)
    query = db.query(Trip)
    
    # Si l'utilisateur a rempli le paramètre origin, on filtre
    if origin:
        query = query.filter(Trip.origin == origin)
        
    # Si l'utilisateur a rempli le paramètre destination, on filtre
    if destination:
        query = query.filter(Trip.destination == destination)
        
    # On exécute la requête et on renvoie une limite de 100 résultats
    return query.limit(100).all()