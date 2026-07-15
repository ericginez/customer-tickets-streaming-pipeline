# ============================================================
# Dockerfile Spark Streaming
# ============================================================
# Ce Dockerfile construit un conteneur permettant d'exécuter
# un job PySpark en streaming connecté à Redpanda (Kafka-compatible).
#
# Il s'appuie sur une image Spark officielle incluant :
# - Spark 4.1.1
# - Scala 2.13
# - Java 17
# - Python 3
# - Ubuntu comme OS de base

FROM spark:4.1.1-scala2.13-java17-python3-ubuntu

# Passage en utilisateur root
# Permet d'avoir les droits nécessaires pour copier des fichiers
# et exécuter certaines commandes si besoin
USER root

# Répertoire de travail dans le conteneur
# Tous les fichiers seront copiés et exécutés depuis /app
WORKDIR /app

# Copie du script de streaming PySpark dans le conteneur
# Ce script contient toute la logique du pipeline :
# - lecture Kafka
# - parsing JSON
# - enrichissement métier
# - agrégations
# - export parquet
COPY streaming.py .

# Commande exécutée au démarrage du conteneur
#
# spark-submit :
# Lance le job Spark
#
# --packages :
# Ajoute dynamiquement le connecteur Kafka nécessaire pour lire Redpanda
# (Spark ne l'inclut pas par défaut)
#
# org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 :
# - version compatible avec Scala 2.13
# - version alignée avec Spark 4.1.1
#
# streaming.py :
# script principal exécuté
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1", "streaming.py"]
