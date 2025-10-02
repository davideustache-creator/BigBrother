# agents/agent_github.py

import os
import time
import requests
import logging
from datetime import datetime
from cassandra.cluster import Cluster
import redis

# --- Configuration du Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration des Connexions ---
# (Noms des services définis dans docker-compose.yml)
CASSANDRA_HOSTS = ['cassandra']
CASSANDRA_KEYSPACE = 'bigbrother'
REDIS_HOST = 'redis-search'
REDIS_PORT = 6379

# --- Configuration de l'API GitHub ---
# Le token est récupéré depuis une variable d'environnement pour la sécurité
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
if not GITHUB_TOKEN:
    raise ValueError("Le token d'accès personnel GitHub n'est pas défini. Veuillez configurer la variable d'environnement GITHUB_TOKEN.")

GITHUB_API_URL = 'https://api.github.com/events'
# Headers pour l'authentification
HEADERS = {'Authorization': f'token {GITHUB_TOKEN}'}
# Délai entre chaque interrogation de l'API (en secondes)
POLL_INTERVAL = 60

def connect_to_cassandra():
    """Tente de se connecter à Cassandra avec plusieurs tentatives."""
    while True:
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            logging.info("Connexion à Cassandra réussie.")
            return session
        except Exception as e:
            logging.warning(f"Connexion à Cassandra échouée, nouvelle tentative dans 5 secondes... Erreur: {e}")
            time.sleep(5)

def connect_to_redis():
    """Tente de se connecter à Redis avec plusieurs tentatives."""
    while True:
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            client.ping()
            logging.info("Connexion à Redis réussie.")
            return client
        except Exception as e:
            logging.warning(f"Connexion à Redis échouée, nouvelle tentative dans 5 secondes... Erreur: {e}")
            time.sleep(5)

def process_and_store_event(event, session, redis_client):
    """Transforme un événement GitHub et le stocke dans Cassandra et Redis."""
    try:
        # --- Transformation des données ---
        event_id = event['id']
        event_type = event['type']
        author = event.get('actor', {}).get('login', 'N/A')
        repo_name = event.get('repo', {}).get('name', 'N/A')
        created_at_str = event.get('created_at', '')
        
        # Conversion de la date
        event_time = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
        event_date = event_time.strftime("%Y-%m-%d")

        title = f"{event_type} par {author} sur {repo_name}"
        content_url = f"https://github.com/{repo_name}"
        
        # --- Stockage dans Cassandra (pour l'archivage) ---
        cql_query = session.prepare("""
            INSERT INTO events (source, event_date, event_time, event_id, event_type, author, title, content_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        session.execute(cql_query, ('github', event_date, event_time, event_id, event_type, author, title, content_url))
        
        # --- Indexation dans Redis (pour la recherche temps réel) ---
        redis_key = f"event:github:{event_id}"
        redis_hash_data = {
            'title': title,
            'author': author,
            'source': 'github',
            'content_url': content_url,
            'event_time': int(event_time.timestamp()) # Stocker le timestamp
        }
        redis_client.hset(redis_key, mapping=redis_hash_data)
        
        logging.info(f"Événement traité et stocké : {title}")

    except Exception as e:
        logging.error(f"Erreur lors du traitement de l'événement {event.get('id', 'N/A')}: {e}")

def main():
    """Fonction principale de l'agent."""
    logging.info("Démarrage de l'agent de collecte GitHub...")
    
    # Établir les connexions aux bases de données
    cassandra_session = connect_to_cassandra()
    redis_client = connect_to_redis()
    
    # Boucle de collecte infinie
    while True:
        try:
            logging.info("Interrogation de l'API GitHub pour de nouveaux événements...")
            response = requests.get(GITHUB_API_URL, headers=HEADERS)
            response.raise_for_status()  # Lève une exception si le statut est une erreur (4xx ou 5xx)
            
            events = response.json()
            logging.info(f"{len(events)} événements reçus de GitHub.")
            
            for event in events:
                process_and_store_event(event, cassandra_session, redis_client)
            
            logging.info(f"Attente de {POLL_INTERVAL} secondes avant la prochaine interrogation.")
            time.sleep(POLL_INTERVAL)

        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur de requête API : {e}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logging.error(f"Une erreur inattendue est survenue: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()