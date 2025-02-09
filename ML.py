# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

(1_000_000 / 163_000_000)

# COMMAND ----------

# Définition des colonnes à garder
feature_columns = [
    'surface_habitable',      # Corrélation avec prix: 0.03, mais importante pour la valeur immobilière
    'type_batiment',          # Peut avoir un impact sur le prix même si corrélation faible (0.00)
    'n_pieces',  
    'id_ville',             # Corrélation avec prix: 0.02, complète l'information de surface
    'departement',            # Corrélation avec prix: 0.01, information géographique importante
    'loyer_m2_appartement',   # Corrélation avec prix: 0.03
    'revenu_fiscal_moyen',    # Corrélation avec prix: 0.03
    'latitude',               # Information géographique
    'longitude',              # Information géographique
    'prix'                    # Notre target
]

# Lecture et échantillonnage des données
df = spark.read.format("delta").table("default.immobilier_analysis").sample(0.323497, 3)
df.display()

# COMMAND ----------

# Nombre de lignes avant nettoyage
print(f"Nombre de lignes avant nettoyage : {df.count()}")

# Voir les nulls avant nettoyage
def analyze_nulls(df):
    total_records = df.count()
    null_stats = []
    
    for column in ["loyer_m2_appartement", "surface_habitable", "n_logements", "n_logements_vacants", "revenu_fiscal_moyen"]:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_stats.append({
            'colonne': column,
            'nulls': null_count,
            'pourcentage_null': round((null_count/total_records) * 100, 2)
        })
    
    return pd.DataFrame(null_stats)

print("\nStatistiques des nulls avant nettoyage:")
display(analyze_nulls(df))

# COMMAND ----------


# Supprimons les lignes où une des colonnes critiques est nulle
df_cleaned = df.dropna(subset=["loyer_m2_appartement", "surface_habitable", "n_logements", "n_logements_vacants","revenu_fiscal_moyen"])

display(df_cleaned)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("default.immobilier_analysis_10M")

# COMMAND ----------

# Supprimons les lignes où une des colonnes critiques est nulle
df_cleaned = df.dropna(subset=["loyer_m2_appartement", "surface_habitable", "n_logements", "n_logements_vacants","revenu_fiscal_moyen",])

display(df_cleaned)

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnull, isnan

nan_count = df.select(
    *[
        count(when(isnull(col(col_name)) | isnan(col(col_name)), col_name)).alias(col_name)
        for col_name in df.columns
    ]
)

display(nan_count)

# COMMAND ----------

df = spark.read.format("delta").table("default.immobilier_analysis_10M")

# COMMAND ----------

# Sélection optimale des colonnes
feature_columns = [
    # Features primaires (meilleures corrélations avec le prix)
    'loyer_m2_appartement',    
    'revenu_fiscal_moyen',     
    'surface_habitable',       
    
    # Features de localisation
    'departement',            
    'latitude',               
    'longitude',              
    
    # Features structurelles
    'type_batiment',          
    'n_pieces'
    # Target
    'prix'                    
]


# Afficher le nombre de lignes initial
print(f"Nombre de lignes initial : {df.count()}")

# Nettoyage des nulls et sélection des colonnes
df_cleaned = df.select(feature_columns).dropna()

# Afficher le nombre de lignes final
print(f"Nombre de lignes après nettoyage : {df_cleaned.count()}")



# COMMAND ----------


from pyspark.sql.functions import col, count, when, isnan

# Vérifier qu'il n'y a plus de nulls
def check_nulls(df):
    total_records = df.count()
    return df.select([
        count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
        for c in df.columns
    ]).show()

print("\nVérification des nulls dans le dataset final:")
check_nulls(df_cleaned)

# Afficher un aperçu des données nettoyées
display(df_cleaned)

# COMMAND ----------

# Définition des colonnes à garder 
feature_columns = [
    'surface_habitable',
    'type_batiment',
    'n_pieces',
    'id_ville',
    'departement',
    'loyer_m2_appartement',
    'revenu_fiscal_moyen',
    'latitude',
    'longitude',
    'prix'
]

# Lecture des données avec sélection des colonnes spécifiques
df = spark.read.format("delta").table("default.immobilier_analysis").select(feature_columns).sample(0.323497, 3)

# Nettoyage des valeurs nulles sur ces colonnes
df_cleaned = df.dropna()

# Afficher le résultat
display(df_cleaned)

# Vérification optionnelle du nombre de lignes
print(f"Nombre de lignes dans le dataset : {df_cleaned.count()}")

# COMMAND ----------

df=df.toPandas()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pyspark.sql.functions import col

# Conversion des colonnes en numérique
cols_to_convert = [
    'surface_habitable',      
    'type_batiment',          
    'n_pieces',               
    'departement',            
    'loyer_m2_appartement',   
    'revenu_fiscal_moyen',    
    'latitude',              
    'longitude',             
    'prix'
]
df = df_cleaned
for col_name in cols_to_convert:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# Convert to Pandas DataFrame for plotting
df_pd = df.toPandas()

# Heatmap des corrélations
df_numeric = df_pd.select_dtypes(include=[np.number])
plt.figure(figsize=(12, 8))
sns.heatmap(df_numeric.corr(), annot=True, fmt=".2f", cmap="coolwarm", linewidths=0.5)
plt.title("Heatmap des Corrélations")
plt.show()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("default.immobilier_analysis_10M")

# COMMAND ----------

(34879 / 2000000)*100

# COMMAND ----------

df_cleaned.filter(col("n_logements").isNull()).display()

# COMMAND ----------

# Définition des colonnes à garder 
feature_columns = [
    'surface_habitable',
    'type_batiment',
    'n_pieces',
    'id_ville',
    'departement',
    'loyer_m2_appartement',
    'revenu_fiscal_moyen',
    'latitude',
    'longitude',
    'prix'
]

# Lecture des données avec sélection des colonnes spécifiques
df = spark.read.format("delta").table("default.immobilier_analysis").select(feature_columns).sample(0.323497, 3)

# Nettoyage des valeurs nulles sur ces colonnes
df_cleaned = df.dropna()

# Afficher le résultat
display(df_cleaned)

# Vérification optionnelle du nombre de lignes
print(f"Nombre de lignes dans le dataset : {df_cleaned.count()}")

# COMMAND ----------


from pyspark.sql.functions import col, count, when, isnan

# Vérifier qu'il n'y a plus de nulls
def check_nulls(df):
    total_records = df.count()
    return df.select([
        count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
        for c in df.columns
    ]).show()

print("\nVérification des nulls dans le dataset final:")
check_nulls(df_cleaned)

# Afficher un aperçu des données nettoyées
display(df_cleaned)

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pyspark.sql.functions import col

# Conversion des colonnes en numérique
cols_to_convert = [
    'surface_habitable',      
    'type_batiment',          
    'n_pieces',               
    'departement',            
    'loyer_m2_appartement',   
    'revenu_fiscal_moyen',    
    'latitude',              
    'longitude',             
    'prix'
]
df = df_cleaned
for col_name in cols_to_convert:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# Convert to Pandas DataFrame for plotting
df_pd = df.toPandas()

# Heatmap des corrélations
df_numeric = df_pd.select_dtypes(include=[np.number])
plt.figure(figsize=(12, 8))
sns.heatmap(df_numeric.corr(), annot=True, fmt=".2f", cmap="coolwarm", linewidths=0.5)
plt.title("Heatmap des Corrélations")
plt.show()

# COMMAND ----------

df_cleaned.write.format("delta").mode("overwrite").saveAsTable("default.immobilier_analysis_50M")
