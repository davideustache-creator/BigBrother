# scripts/initialize_db.py (Version finale corrigée grâce à vous)

import time
import redis
from cassandra.cluster import Cluster # Assurez-vous que le paquet python-cassandra-driver est installé : pip install cassandra-driver
from cassandra.protocol import ProtocolException
from redis.exceptions import ResponseError

# Correction : L'import correct pour IndexDefinition et IndexType
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, TagField, NumericField

# --- Configuration ---
CASSANDRA_HOSTS = ['cassandra']
CASSANDRA_KEYSPACE = 'bigbrother'
REDIS_HOST = 'redis-search'
REDIS_PORT = 6379

def create_cassandra_schema(session):
    """Crée le keyspace et la table events dans Cassandra."""
    print("Création du schéma Cassandra...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
    """)
    print(f"Keyspace '{CASSANDRA_KEYSPACE}' créé ou déjà existant.")
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute("""
        CREATE TABLE IF NOT EXISTS events (
            source text, event_date text, event_time timestamp, event_id text,
            event_type text, author text, title text, content_url text,
            metadata map<text, text>,
            PRIMARY KEY ((source, event_date), event_time, event_id)
        ) WITH CLUSTERING ORDER BY (event_time DESC);
    """)
    print("Table 'events' créée ou déjà existante.")

def create_redisearch_index(client):
    """Crée ou recrée l'index RediSearch pour s'assurer qu'il est à jour."""
    print("Mise à jour de l'index RediSearch...")
    
    index_name = 'idx:events'
    key_prefix = 'event:'
    
    # Le schéma à jour que nous voulons
    schema = (
        TextField("title", weight=5.0),
        TextField("content"),
        TagField("author"),
        TagField("source"),
        NumericField("event_time", sortable=True)
    )
        
    definition = IndexDefinition(prefix=[key_prefix], index_type=IndexType.HASH)

    # --- NOUVELLE LOGIQUE : On supprime l'index s'il existe ---
    try:
        client.ft(index_name).dropindex(delete_documents=True)
        print(f"Ancien index '{index_name}' trouvé et supprimé.")
    except ResponseError:
        # C'est normal si l'index n'existait pas au premier lancement
        print(f"Aucun index existant '{index_name}' à supprimer.")
    
    # --- On crée le nouvel index avec le schéma à jour ---
    print(f"Création du nouvel index '{index_name}'...")
    client.ft(index_name).create_index(fields=schema, definition=definition)
    print(f"Index RediSearch '{index_name}' créé avec le dernier schéma.")


def main():
    print("--- Démarrage de l'initialisation des bases de données ---")
    
    print("Connexion à Cassandra...")
    # ... (Le reste du code est inchangé et correct)
    while True:
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect()
            print("Connecté à Cassandra !")
            break
        except Exception as e:
            print(f"En attente de Cassandra... ({e})")
            time.sleep(5)
    create_cassandra_schema(session)
    session.shutdown()
    
    print("\nConnexion à Redis...")
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()
        print("Connecté à Redis !")
    except Exception as e:
        print(f"Impossible de se connecter à Redis: {e}")
        return
        
    create_redisearch_index(redis_client)
    
    print("\n--- Initialisation terminée avec succès ! ---")

if __name__ == "__main__":
    main()