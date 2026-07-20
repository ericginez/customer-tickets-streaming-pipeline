# Pipeline streaming de tickets clients avec Redpanda et Apache Spark

Projet réalisé dans le cadre du parcours **Data Engineer OpenClassrooms**.

L’objectif est de concevoir un pipeline événementiel conteneurisé capable
de générer, transporter, agréger et stocker en continu des tickets clients.

L’architecture repose sur :

- **Redpanda** comme broker compatible avec l’API Kafka ;
- un **producteur Python** générant les tickets clients ;
- **Apache Spark Structured Streaming** pour leur consommation et leur agrégation ;
- les formats **JSON** et **Parquet** pour la restitution des résultats ;
- **Docker Compose** pour orchestrer l’ensemble des composants.

## Contexte

Une organisation reçoit en continu des demandes formulées par ses clients.

Ces tickets doivent être traités rapidement afin de produire des indicateurs
exploitables par les équipes opérationnelles et analytiques.

Une architecture exclusivement batch imposerait d’attendre l’exécution
planifiée d’un traitement. Ce projet met donc en œuvre une chaîne de streaming
capable de traiter les événements au fil de leur arrivée.

## Présentation

- [Consulter la présentation de soutenance au format PDF](presentation/projet-09-pipeline-streaming-tickets-clients.pdf)

## Objectifs

Le projet permet de :

- générer des tickets clients avec Python ;
- sérialiser les événements au format JSON ;
- publier les messages dans un topic Redpanda ;
- utiliser un broker compatible avec Kafka ;
- consommer les événements avec Spark Structured Streaming ;
- appliquer un schéma structuré aux messages ;
- agréger les tickets par type de demande ;
- écrire les résultats aux formats JSON et Parquet ;
- conserver l’état du traitement avec des checkpoints ;
- exécuter l’ensemble dans des conteneurs Docker ;
- rendre l’infrastructure locale reproductible.

## Architecture

```text
┌──────────────────────────┐
│    Producteur Python     │
│                          │
│ producer/producer.py     │
│ Génération des tickets   │
└─────────────┬────────────┘
              │ Messages JSON
              ▼
┌──────────────────────────┐
│        Redpanda          │
│                          │
│ Broker compatible Kafka  │
│ Topic : client_tickets   │
└─────────────┬────────────┘
              │ Flux d’événements
              ▼
┌───────────────────────────────┐
│ Spark Structured Streaming    │
│                               │
│ spark/streaming.py            │
│ - lecture du topic            │
│ - désérialisation JSON        │
│ - application du schéma       │
│ - agrégation par type         │
│ - suivi des offsets           │
└──────────────┬────────────────┘
               │
               ├──► output/tickets_by_type_json
               ├──► output/tickets_by_type_parquet
               └──► checkpoints
```

## Technologies utilisées

| Technologie | Utilisation |
|---|---|
| Python | Génération des tickets clients |
| Redpanda | Broker de messages compatible Kafka |
| Kafka API | Communication entre le producteur et Spark |
| Apache Spark | Traitement distribué des événements |
| Spark Structured Streaming | Consommation et agrégation continue |
| JSON | Sérialisation des messages et export lisible |
| Parquet | Export colonnaire optimisé pour l’analyse |
| Docker | Conteneurisation des composants |
| Docker Compose | Orchestration locale des services |
| Git et GitHub | Gestion des versions et publication |

## Arborescence

```text
.
├── .gitignore
├── README.md
├── docker-compose.yml
├── checkpoints/          # généré à l’exécution, non versionné
├── documentation/
│   ├── Architecture_hybride.drawio
│   └── Justification_architecture_hybride.pdf
├── output/               # généré à l’exécution, non versionné
├── presentation/
│   └── projet-09-pipeline-streaming-tickets-clients.pdf
├── producer/
│   ├── Dockerfile
│   └── producer.py
└── spark/
    ├── Dockerfile
    ├── read_parquet.py
    └── streaming.py
```

Les fichiers contenus dans `output` et `checkpoints` sont générés pendant
l’exécution et ne sont pas versionnés dans Git.

## Services Docker

Le fichier `docker-compose.yml` orchestre trois services.

### Redpanda

Le service `redpanda` utilise l’image officielle :

```text
redpandadata/redpanda:latest
```

Il écoute sur le port Kafka :

```text
9092
```

À l’intérieur du réseau Docker, les autres services se connectent à :

```text
redpanda:9092
```

La configuration utilise un seul nœud et des ressources limitées afin de
rester adaptée à un POC local.

### Producer

Le service `producer` est construit à partir du dossier :

```text
producer/
```

Le script `producer.py` génère les tickets clients et les publie dans
Redpanda sous forme de messages JSON.

Les tickets contiennent notamment :

- `ticket_id` ;
- `client_id` ;
- `created_at` ;
- `request` ;
- `request_type` ;
- `priority`.

### Spark

Le service `spark` est construit à partir du dossier :

```text
spark/
```

Le script `streaming.py` consomme les événements, applique les
transformations et écrit les agrégats dans les dossiers de sortie.

Deux volumes sont montés :

```text
./output      → /app/output
./checkpoints → /app/checkpoints
```

Les résultats restent ainsi accessibles sur la machine hôte après l’arrêt
du conteneur.

## Topic Redpanda

Les événements sont publiés dans le topic :

```text
client_tickets
```

Ce topic découple la génération des tickets de leur traitement.

Le producteur publie les événements indépendamment du rythme de consommation
de Spark.

## Structure des tickets

Les tickets générés comportent notamment les champs suivants :

```json
{
  "ticket_id": "TICKET-0001",
  "client_id": "CLIENT-0102",
  "created_at": "2026-01-15T14:32:00Z",
  "request": "Le client ne parvient pas à accéder à son compte.",
  "request_type": "support_technique",
  "priority": "haute"
}
```

Le contenu exact et les valeurs possibles sont définis dans
`producer/producer.py` et dans le schéma Spark de `spark/streaming.py`.

## Fonctionnement du pipeline

### Génération des tickets

Le script `producer/producer.py` génère des événements représentant des
tickets clients.

Chaque événement est :

1. construit sous forme de dictionnaire Python ;
2. sérialisé au format JSON ;
3. publié dans le topic `client_tickets`.

### Transport avec Redpanda

Redpanda reçoit les événements et les conserve dans le topic.

Sa compatibilité avec Kafka permet au producteur et à Spark d’utiliser
les protocoles et connecteurs de l’écosystème Kafka.

### Consommation avec Spark

Le script `spark/streaming.py` consomme les messages présents dans le topic.

Le traitement réalise notamment :

- la lecture des messages Kafka ;
- la conversion des valeurs binaires ;
- la désérialisation du JSON ;
- l’application d’un schéma explicite ;
- la sélection des champs utiles ;
- l’agrégation des tickets par type de demande ;
- l’écriture incrémentale des résultats.

### Export JSON

Les résultats JSON sont écrits dans :

```text
output/tickets_by_type_json/
```

Ce format facilite :

- la lecture manuelle ;
- le contrôle des résultats ;
- le débogage ;
- les échanges avec d’autres applications.

### Export Parquet

Les résultats Parquet sont écrits dans :

```text
output/tickets_by_type_parquet/
```

Parquet offre :

- un stockage colonnaire ;
- une compression efficace ;
- une lecture analytique performante ;
- une bonne intégration avec Spark.

### Checkpoints

Les checkpoints sont stockés dans :

```text
checkpoints/
```

Ils permettent à Spark de mémoriser :

- les offsets déjà consommés ;
- la progression du flux ;
- l’état du traitement ;
- les informations nécessaires à la reprise.

## Prérequis

L’environnement nécessite :

- Docker Desktop ;
- Docker Compose ;
- Git ;
- suffisamment de mémoire pour exécuter Redpanda et Spark ;
- le port `9092` disponible sur la machine.

Vérifier les outils :

```powershell
docker --version
docker compose version
git --version
```

## Lancement du pipeline

Depuis la racine du projet :

```powershell
docker compose up -d --build
```

Cette commande :

1. construit l’image du producteur ;
2. construit l’image Spark ;
3. démarre Redpanda ;
4. démarre le producteur ;
5. démarre le traitement Spark.

Vérifier l’état des services :

```powershell
docker compose ps
```

## Consultation des journaux

Afficher tous les journaux :

```powershell
docker compose logs -f
```

Afficher uniquement les journaux Redpanda :

```powershell
docker compose logs -f redpanda
```

Afficher les journaux du producteur :

```powershell
docker compose logs -f producer
```

Afficher les journaux Spark :

```powershell
docker compose logs -f spark
```

## Vérification de Redpanda

Lister les topics :

```powershell
docker compose exec redpanda rpk topic list
```

Décrire le topic :

```powershell
docker compose exec redpanda rpk topic describe client_tickets
```

Consommer quelques événements :

```powershell
docker compose exec redpanda rpk topic consume client_tickets -n 5
```

## Vérification des résultats

Afficher les sorties générées :

```powershell
tree output /F /A
```

Les deux dossiers attendus sont :

```text
output/
├── tickets_by_type_json/
└── tickets_by_type_parquet/
```

Spark génère plusieurs fichiers `part-*`, car les données sont écrites
de manière distribuée.

Les fichiers `.crc` et `_SUCCESS` sont des fichiers techniques générés
par Hadoop et Spark.

## Lecture des résultats Parquet

Le script suivant permet de lire les fichiers Parquet générés :

```text
spark/read_parquet.py
```

Selon son mode d’exécution, il peut être lancé dans le conteneur Spark :

```powershell
docker compose exec spark python /app/read_parquet.py
```

Le chemin exact dépend du `WORKDIR` et des instructions définies dans
`spark/Dockerfile`.

## Arrêt de l’environnement

Arrêter les services :

```powershell
docker compose down
```

Arrêter les services et supprimer les volumes Docker :

```powershell
docker compose down -v
```

Les dossiers locaux `output` et `checkpoints` restent présents, car ils
sont montés depuis la machine hôte.

## Réinitialisation du traitement

Pour rejouer le pipeline depuis le début, arrêter les services puis supprimer
les résultats et les checkpoints :

```powershell
docker compose down

Remove-Item output\tickets_by_type_json -Recurse -Force `
    -ErrorAction SilentlyContinue

Remove-Item output\tickets_by_type_parquet -Recurse -Force `
    -ErrorAction SilentlyContinue

Get-ChildItem checkpoints -Force |
    Where-Object { $_.Name -ne ".gitkeep" } |
    Remove-Item -Recurse -Force
```

Relancer ensuite :

```powershell
docker compose up -d --build
```

La suppression des checkpoints entraîne une nouvelle consommation du flux
selon la configuration Spark et la politique des offsets.

## Résultats obtenus

La version finale du prototype permet :

- de générer des tickets clients avec Python ;
- de publier les événements dans Redpanda ;
- d’utiliser le topic `client_tickets` ;
- de consommer les messages avec Spark Structured Streaming ;
- d’appliquer un schéma structuré aux données JSON ;
- d’agréger les tickets par type de demande ;
- de produire une sortie JSON ;
- de produire une sortie Parquet compressée avec Snappy ;
- de conserver la progression du traitement avec des checkpoints ;
- d’exécuter l’ensemble dans trois services Docker.

## Livrables

Le projet comprend :

- un producteur Python générant des tickets clients en continu ;
- un topic Redpanda nommé `client_tickets` ;
- un pipeline Spark Structured Streaming consommant et agrégeant les événements ;
- un schéma explicite appliqué aux messages JSON ;
- une agrégation des tickets par type de demande ;
- des sorties analytiques aux formats JSON et Parquet ;
- une compression Parquet avec Snappy ;
- des checkpoints assurant le suivi des offsets et la reprise du traitement ;
- trois services conteneurisés : Redpanda, producteur Python et Spark ;
- un fichier `docker-compose.yml` orchestrant l’environnement local ;
- des `Dockerfile` dédiés au producteur et au traitement Spark ;
- un script permettant de lire les résultats Parquet ;
- une documentation d’architecture hybride ;
- une présentation de soutenance au format PDF ;
- une documentation technique complète dans le README ;
- un dépôt GitHub public documenté.

## Limites et évolutions possibles

Le projet constitue un POC local. Des évolutions seraient nécessaires
pour un environnement de production :

- figer les versions des images Docker ;
- ajouter des healthchecks ;
- attendre explicitement que Redpanda soit prêt ;
- utiliser plusieurs partitions ;
- configurer la réplication ;
- ajouter un registre de schémas ;
- mettre en place une dead-letter queue ;
- superviser le débit et le retard de consommation ;
- ajouter l’authentification ;
- chiffrer les communications ;
- déployer les composants dans le cloud ;
- stocker les résultats dans un data lake ;
- intégrer les fichiers Parquet dans un lakehouse.

## Compétences démontrées

- conception d’une architecture événementielle ;
- production de messages avec Python ;
- utilisation d’un broker compatible Kafka ;
- gestion d’un topic Redpanda ;
- traitement avec Spark Structured Streaming ;
- application d’un schéma explicite ;
- agrégation de données en continu ;
- production de fichiers JSON et Parquet ;
- utilisation de la compression Snappy ;
- gestion des checkpoints et des offsets ;
- conteneurisation avec Docker ;
- orchestration avec Docker Compose ;
- diagnostic d’une infrastructure distribuée ;
- documentation d’un POC Data Engineering.

## Auteur

**Eric Ginez**  
Parcours Data Engineer — OpenClassrooms