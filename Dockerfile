# Dockerfile

# Utiliser une image Python officielle et légère
FROM python:3.10-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier des dépendances D'ABORD pour profiter du cache Docker
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le reste du code du projet dans le conteneur
COPY . .

# La commande par défaut pour lancer l'application sera définie dans docker-compose