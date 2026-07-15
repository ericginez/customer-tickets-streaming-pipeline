# Image de base utilisée pour exécuter le producer Python.
# Ici, on utilise une image Python 3.10 "slim" :
# - plus légère qu'une image Python complète ;
# - suffisante pour lancer un script simple ;
# - adaptée à un POC conteneurisé.
FROM python:3.10-slim

# Définit le répertoire de travail dans le conteneur.
# Toutes les commandes suivantes seront exécutées depuis /app.
# Cela permet d'organiser proprement le code applicatif dans l'image.
WORKDIR /app

# Copie le script producer.py depuis le dossier local vers le conteneur.
# Le fichier est copié dans /app/producer.py grâce au WORKDIR défini plus haut.
COPY producer.py .

# Installe les dépendances Python nécessaires au producer :
# - kafka-python : permet d'envoyer des messages vers Redpanda via l'API Kafka ;
# - faker : permet de générer des données de tickets réalistes pour le POC.
#
# Pour un projet plus industrialisé, on pourrait utiliser un fichier requirements.txt
# afin de mieux figer les versions des dépendances.
RUN pip install kafka-python faker

# Commande exécutée au démarrage du conteneur.
# Elle lance le script producer.py, qui génère les tickets et les publie
# dans le topic Redpanda configuré dans le script.
CMD ["python", "producer.py"]
