# ===============
# PRODUCER PYTHON
# ===============
# Génère des tickets clients simulés et les envoie dans Redpanda (Kafka)

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Configuration Kafka / Redpanda
TOPIC = "client_tickets"
BOOTSTRAP_SERVERS = "redpanda:9092"

# Valeurs métier simulées
TICKET_TYPES = ["bogue", "demande_info", "reclamation", "incident"]
PRIORITIES = ["low", "medium", "high", "critical"]

# ====================
# CONNEXION AVEC RETRY
# ====================
# Permet d'attendre que Redpanda soit disponible

producer = None
while producer is None:
    try:
        print("Connexion à Redpanda...")
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connexion OK")
    except Exception as e:
        print("Retry dans 5s...", e)
        time.sleep(5)

# =====================
# GENERATION DE DONNEES
# =====================

def generate_ticket():
    return {
        "ticket_id": fake.uuid4(),
        "client_id": fake.random_int(min=1000, max=9999),
        "created_at": datetime.utcnow().isoformat(),
        "request": fake.sentence(nb_words=8),
        "type": random.choice(TICKET_TYPES),
        "priority": random.choice(PRIORITIES)
    }

print("Streaming en cours...")

# ==============
# BOUCLE INFINIE
# ==============

while True:
    try:
        ticket = generate_ticket()
        producer.send(TOPIC, ticket)
        producer.flush()  # garantit l'envoi immédiat
        print("Ticket envoyé:", ticket)
        time.sleep(1)  # 1 événement / seconde

    except Exception as e:
        print("Erreur, reconnexion...", e)
        producer = None
