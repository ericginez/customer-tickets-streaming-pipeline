from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, window
from pyspark.sql.types import StructType, StructField, StringType

import shutil
import os


# ==============
# CHEMINS DOCKER
# ==============
# Définition des chemins internes au conteneur Docker
# Ces chemins correspondent aux volumes montés dans docker-compose

OUTPUT_JSON_PATH = "/app/output/tickets_by_type_json"  # sortie JSON (debug / lecture simple)
OUTPUT_PARQUET_PATH = "/app/output/tickets_by_type_parquet"  # sortie principale analytique

CHECKPOINT_JSON_PATH = "/app/checkpoints/export_json"  # checkpoint pour export JSON
CHECKPOINT_PARQUET_PATH = "/app/checkpoints/export_parquet"  # checkpoint pour export parquet

REDPANDA_BOOTSTRAP_SERVERS = "redpanda:9092"  # adresse du broker Kafka (Redpanda)
TOPIC = "client_tickets"  # topic Kafka à consommer


# ================================================
# SUPPRESSION DES SORTIES DE LA SESSION PRECEDENTE
# ================================================
# Nettoyage des anciens outputs pour éviter conflits ou incohérences

for path in [
    OUTPUT_JSON_PATH,
    OUTPUT_PARQUET_PATH,
]:
    if os.path.exists(path):
        shutil.rmtree(path)


# ============================
# CREATION DE LA SESSION SPARK
# ============================
# Initialisation de Spark avec configuration du parallélisme

spark = (
    SparkSession.builder
    .appName("ClientTicketsStreaming")  # nom du job Spark
    .config("spark.sql.shuffle.partitions", "4")  # nombre de partitions (performance)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =================================
# DEFINITION DU SCHEMA DES MESSAGES
# =================================
# Schéma attendu des messages JSON envoyés par le producer

ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("request", StringType(), True),
    StructField("type", StringType(), True),
    StructField("priority", StringType(), True)
])


# =================================
# LECTURE DU STREAM DEPUIS REDPANDA
# =================================
# Spark lit le topic Kafka comme un stream continu

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", REDPANDA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# =============================
# EXTRACTION ET PARSING DU JSON
# =============================
# Conversion du binaire Kafka -> string -> JSON structuré

json_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_value")

parsed_stream = json_stream.select(
    from_json(col("json_value"), ticket_schema).alias("ticket")
)

tickets_stream = (
    parsed_stream.select("ticket.*")
    .withColumn("created_at", to_timestamp(col("created_at")))
)

tickets_stream = tickets_stream.filter(
    col("ticket_id").isNotNull() &
    col("client_id").isNotNull() &
    col("created_at").isNotNull() &
    col("type").isNotNull() &
    col("priority").isNotNull()
)


# ============================================================
# TRANSFORMATION METIER : AFFECTATION AUTOMATIQUE D’UNE EQUIPE
# ============================================================
# Enrichissement métier : mapping type -> équipe support

enriched_stream = tickets_stream.withColumn(
    "support_team",
    when(col("type").isin("bogue", "incident"), "support technique")
    .when(col("type") == "reclamation", "service client")
    .otherwise("support general")
)


# ========================
# AGREGRATIONS TEMPORELLES
# ========================
# Agrégations avec fenêtres de 1 minute (streaming analytics)

tickets_by_type = enriched_stream.groupBy(
    window(col("created_at"), "1 minute"),
    col("type")
).count().orderBy(col("count").desc())

tickets_by_priority = enriched_stream.groupBy(
    window(col("created_at"), "1 minute"),
    col("priority")
).count().orderBy(col("count").desc())

tickets_by_team = enriched_stream.groupBy(
    window(col("created_at"), "1 minute"),
    col("support_team")
).count().orderBy(col("count").desc())


# ===============
# SORTIES CONSOLE
# ===============
# Visualisation en temps réel

query_type = (
    tickets_by_type.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query_priority = (
    tickets_by_priority.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query_team = (
    tickets_by_team.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)


# ===========
# EXPORT JSON
# ===========

def export_tickets_by_type_json(batch_df, batch_id):
    (
        batch_df.write
        .mode("overwrite")
        .json(OUTPUT_JSON_PATH)
    )


query_export_json = (
    tickets_by_type.writeStream
    .outputMode("complete")
    .foreachBatch(export_tickets_by_type_json)
    .option("checkpointLocation", CHECKPOINT_JSON_PATH)
    .start()
)


# ==============
# EXPORT PARQUET
# ==============

def export_tickets_by_type_parquet(batch_df, batch_id):
    (
        batch_df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(OUTPUT_PARQUET_PATH)
    )


query_export_parquet = (
    tickets_by_type.writeStream
    .outputMode("complete")
    .foreachBatch(export_tickets_by_type_parquet)
    .option("checkpointLocation", CHECKPOINT_PARQUET_PATH)
    .start()
)


# ======================================
# MAINTIEN DE L’APPLICATION EN EXECUTION
# ======================================

spark.streams.awaitAnyTermination()
