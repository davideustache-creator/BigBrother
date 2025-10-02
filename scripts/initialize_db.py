# scripts/initialize_db.py (Version finale corrigée grâce à vous)

import time
import redis
from cassandra.cluster import Cluster
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
    """Crée l'index RediSearch pour les événements."""
    print("Création de l'index RediSearch...")
    index_name = 'idx:events'
    key_prefix = 'event:'
    schema = (
        TextField("title", weight=5.0), TagField("author"), TagField("source"),
        TextField("content_url"), NumericField("event_time", sortable=True)
    )
    definition = IndexDefinition(prefix=[key_prefix], index_type=IndexType.HASH)
    try:
        client.ft(index_name).create_index(fields=schema, definition=definition)
        print(f"Index RediSearch '{index_name}' créé.")
    except ResponseError as e:
        if "Index already exists" in str(e):
            print(f"L'index RediSearch '{index_name}' existe déjà.")
        else:
            raise e

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