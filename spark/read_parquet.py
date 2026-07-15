# ===============
# LECTURE PARQUET
# ===============
# Permet d'analyser les résultats exportés par Spark

import pandas as pd

# Lecture de tous les fichiers parquet du dossier
df = pd.read_parquet("../output/tickets_by_type_parquet")

# Extraction des bornes de fenêtre
df["window_start"] = df["window"].apply(lambda x: x["start"])
df["window_end"] = df["window"].apply(lambda x: x["end"])

# Suppression de la colonne complexe
df = df.drop(columns=["window"])

# Conversion en datetime
df["window_start"] = pd.to_datetime(df["window_start"])
df["window_end"] = pd.to_datetime(df["window_end"])

print(df)
