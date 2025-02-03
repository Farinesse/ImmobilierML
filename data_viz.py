# Databricks notebook source
df = spark.read.format("delta").table("default.immobilier_analysis_1M")

# COMMAND ----------

df=df.toPandas()



# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

df.info()

# COMMAND ----------

!pip install ace-tools

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Charger le DataFrame
# df = pd.read_csv("fichier.csv")  # Décommentez pour charger un fichier

# Conversion des colonnes en numérique
cols_to_convert = [
    "surface_habitable", "n_pieces", "latitude", "longitude",
    "loyer_m2_appartement", "loyer_m2_maison", "revenu_fiscal_moyen",
    "n_logements", "n_logements_vacants", "prix"
]
df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

# Suppression des valeurs nulles après conversion
df_clean = df.dropna()

# Filtrage des valeurs aberrantes (1er au 99e percentile)
def filter_outliers(df, column):
    q1, q99 = np.percentile(df[column].dropna(), [1, 99])
    return df[(df[column] >= q1) & (df[column] <= q99)]

df_filtered = filter_outliers(df_clean, "prix")
df_filtered = filter_outliers(df_filtered, "surface_habitable")

def plot_histogram(data, column, bins=50, color='blue', xlabel='', title=''):
    plt.figure(figsize=(12, 5))
    plt.hist(data[column], bins=bins, edgecolor="black", alpha=0.7, color=color)
    plt.xlabel(xlabel)
    plt.ylabel("Nombre de biens")
    plt.title(title)
    plt.grid(True)
    plt.show()

# Histogramme des prix des biens
plot_histogram(df_filtered, "prix", xlabel="Prix (€)", title="Distribution des prix des biens immobiliers")

# Histogramme des surfaces habitables
plot_histogram(df_filtered, "surface_habitable", color='green', xlabel="Surface Habitable (m²)", title="Distribution des surfaces habitables")

# Évolution du prix moyen par année
prix_par_annee = df_clean.groupby("annee")["prix"].mean()
plt.figure(figsize=(10, 5))
plt.plot(prix_par_annee.index, prix_par_annee.values, marker="o", linestyle="-")
plt.xlabel("Année")
plt.ylabel("Prix moyen (€)")
plt.title("Évolution du prix moyen des biens immobiliers par année")
plt.grid(True)
plt.show()


# Relation Surface habitable vs Prix
plt.figure(figsize=(10, 5))
plt.scatter(df_clean["surface_habitable"], df_clean["prix"], alpha=0.5)
plt.xlabel("Surface Habitable (m²)")
plt.ylabel("Prix (€)")
plt.ticklabel_format(style='plain', axis='y')  # Affichage du prix réel
plt.title("Relation entre la surface habitable et le prix des biens")
plt.grid(True)
plt.show()

# Relation Surface habitable vs Prix bettween 0 and 20 millions
plt.figure(figsize=(10, 5))
plt.scatter(df_clean["surface_habitable"], df_clean["prix"], alpha=0.5)
plt.xlabel("Surface Habitable (m²)")
plt.ylabel("Prix (€)")
plt.ticklabel_format(style='plain', axis='y')  # Affichage du prix réel
plt.title("Relation entre la surface habitable et le prix des biens entre 0 et 20 millions")
plt.ylim(0, 20000000)  # Fixer l'ordonnée de 0 à 60 millions
plt.grid(True)
plt.show()

# Prix moyen des biens par département (Top 10)
prix_par_departement = df_clean.groupby("departement")["prix"].mean().nlargest(10)
plt.figure(figsize=(10, 5))
prix_par_departement.plot(kind="bar", color="skyblue", edgecolor="black")
plt.xlabel("Département")
plt.ylabel("Prix moyen (€)")
plt.title("Top 10 des départements avec les prix moyens les plus élevés")
plt.xticks(rotation=45)
plt.show()

# Heatmap des corrélations
df_numeric = df.select_dtypes(include=[np.number])
plt.figure(figsize=(12, 8))
sns.heatmap(df_numeric.corr(), annot=True, fmt=".2f", cmap="coolwarm", linewidths=0.5)
plt.title("Heatmap des Corrélations")
plt.show()

# Répartition du nombre de pièces
plt.figure(figsize=(10, 5))
sns.countplot(x=df_clean["n_pieces"], order=df_clean["n_pieces"].value_counts().index, palette="viridis")
plt.xlabel("Nombre de pièces")
plt.ylabel("Nombre de biens")
plt.title("Répartition du nombre de pièces des biens immobiliers")
plt.grid(True)
plt.show()

# Distribution des loyers au m²
for col, color, label in zip(["loyer_m2_maison", "loyer_m2_appartement"], ["blue", "green"], ["Maison", "Appartement"]):
    df_filtered = filter_outliers(df_clean, col)
    plot_histogram(df_filtered, col, xlabel=f"Loyer au m² (€) - {label}", title=f"Distribution du loyer au m² des {label.lower()}s", color=color)


# COMMAND ----------


