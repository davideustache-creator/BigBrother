# app/main.py

import os
import redis
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# Importe les classes nécessaires pour construire des requêtes RediSearch
from redis.commands.search.query import Query

# --- Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-search')
REDIS_PORT = 6379
REDIS_INDEX_NAME = 'idx:events'

# Variable globale pour le client Redis
redis_client = None

# --- Gestion du cycle de vie de l'application ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Cette fonction gère les actions à effectuer au démarrage et à l'arrêt de l'API.
    C'est la manière moderne de gérer les connexions aux bases de données avec FastAPI.
    """
    global redis_client
    # Action au démarrage : connexion à Redis
    print("Connexion à Redis...")
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        redis_client.ping()
        print("Connecté à Redis avec succès !")
    except redis.exceptions.ConnectionError as e:
        print(f"Erreur de connexion à Redis: {e}")
        # Dans une vraie application, on pourrait décider d'arrêter le démarrage ici
        
    yield  # L'application tourne ici
    
    # Action à l'arrêt : fermeture de la connexion
    print("Fermeture de la connexion Redis.")
    redis_client.close()

# --- Initialisation de l'application FastAPI ---
app = FastAPI(title="BigBrother API", lifespan=lifespan)


# --- Définition des Endpoints de l'API ---
@app.get("/")
def read_root():
    """Endpoint racine pour vérifier que l'API est en ligne."""
    return {"Project": "BigBrother-RediSearch", "Status": "Online"}

@app.get("/api/search")
def search_events(q: str | None = None):
    """
    Endpoint de recherche.
    Il accepte un paramètre de requête 'q' (ex: /api/search?q=python).
    """
    if not q:
        raise HTTPException(status_code=400, detail="Le paramètre de requête 'q' est manquant.")

    try:
        # Construit une requête de recherche simple sur l'index
        # On limite les résultats aux 50 premiers documents
        query = Query(q).limit(0, 50)
        
        # Exécute la recherche sur notre index
        result = redis_client.ft(REDIS_INDEX_NAME).search(query)
        
        # Formate les résultats pour une réponse JSON propre
        formatted_results = []
        for doc in result.docs:
            # On transforme l'objet Document de Redis en un dictionnaire simple
            # On omet certains champs internes comme 'id' et 'payload' pour la clarté
            formatted_results.append({
                'title': doc.title,
                'content': doc.content,
                'author': doc.author,
                'source': doc.source,
                'event_time': datetime.fromtimestamp(float(doc.event_time)).isoformat()
            })
            
        return {"total_results": result.total, "results": formatted_results}

    except Exception as e:
        logging.error(f"Erreur lors de la recherche pour la requête '{q}': {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur lors de la recherche.")