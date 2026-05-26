# ============================================================================
# PROJET : Analyse des plaintes des consommateurs - CFPB
# AUTEUR : Billazy Data Engineer, Data Analyst
# VERSION : 1.0
# OBJECTIF : Transformer les données brutes en indicateurs de qualité service client
# ============================================================================

import pandas as pd
import subprocess
import zipfile
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.window import Window





# ============================================================================
# ÉTAPE 1 : INGESTION DES DONNÉES
# ============================================================================
# Objectif : Récupérer les données publiques du CFPB (1.7 Go compressé)
# Enjeu : Gérer un gros volume avec mémoire limitée (version gratuite)
# Solution : Streaming par chunks avec pandas

csv_url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
print("Téléchargement de la base des plaintes CFPB...")

# Téléchargement via wget (robuste et reprend les interruptions)
subprocess.run(["wget", "-O", "/tmp/complaints.csv.zip", csv_url])

# Décompression
with zipfile.ZipFile("/tmp/complaints.csv.zip", "r") as zip_ref:
    zip_ref.extractall("/tmp/")

# Lecture par chunks de 10k lignes (économie mémoire)
chunk_size = 10000
chunks = []

for i, chunk in enumerate(pd.read_csv("/tmp/complaints.csv", chunksize=chunk_size, nrows=50000)):
    chunks.append(chunk)
    print(f"Chunk {i+1} chargé: {len(chunk)} lignes")

# Conversion en DataFrame Spark pour analyses distribuées
df = spark.createDataFrame(pd.concat(chunks))
print(f"{df.count()} plaintes chargées | {len(df.columns)} colonnes")





# ============================================================================
# ÉTAPE 2 : NETTOYAGE ET PRÉPARATION
# ============================================================================
# Objectif : Standardiser les données pour l'analyse
# Enjeu : Gérer les valeurs manquantes, normaliser les formats

# Renommage des colonnes (français pour clarté métier)
df_clean = (df
    .withColumnRenamed("Date received", "date_reception")
    .withColumnRenamed("Product", "produit")
    .withColumnRenamed("Issue", "probleme")
    .withColumnRenamed("Company", "entreprise")
    .withColumnRenamed("State", "etat")
    .withColumnRenamed("Company response to consumer", "reponse_entreprise")
    .withColumnRenamed("Timely response?", "reponse_delai")
    .withColumnRenamed("Consumer disputed?", "conteste")
    .withColumnRenamed("Date sent to company", "date_envoi_entreprise")
)




# Nettoyage des valeurs
df_clean = (df_clean
    .withColumn("produit", trim(col("produit")))
    .withColumn("entreprise", trim(col("entreprise")))
    .withColumn("etat", when(col("etat") == "", lit("Non spécifié")).otherwise(col("etat")))
    .withColumn("reponse_delai", 
        when(col("reponse_delai") == "Yes", lit("Oui"))
        .when(col("reponse_delai") == "No", lit("Non"))
        .otherwise(lit("Inconnu")))
    .withColumn("conteste", 
        when(col("conteste") == "Yes", lit("Oui"))
        .when(col("conteste") == "No", lit("Non"))
        .otherwise(lit("Inconnu")))
)



# ============================================================================
# ÉTAPE 3 : CRÉATION D'INDICATEURS DE PERFORMANCE
# ============================================================================
# Objectif : Construire des KPIs actionnables pour le pilotage
# Métriques clés : Délai réponse, conformité, contestation

df_analysis = (df_clean
    # Calcul du délai de réponse (jours) - KPI opérationnel
    .withColumn("delai_reponse", 
        when(col("date_envoi_entreprise").isNotNull() & col("date_reception").isNotNull(),
             datediff(to_date(col("date_envoi_entreprise")), to_date(col("date_reception"))))
        .otherwise(lit(None)))
    
    # Flag conformité (réponse dans les délais légaux)
    .withColumn("reponse_qualite",
        when(col("reponse_delai") == "Oui", 1).otherwise(0))
    
    # Flag contestation (insatisfaction client)
    .withColumn("a_conteste",
        when(col("conteste") == "Oui", 1).otherwise(0))
    
    # Score qualité composite (10 = parfait, -5 = critique)
    .withColumn("score_qualite",
        when(col("reponse_delai") == "Oui", 10)
        .when(col("reponse_delai") == "Non", -5)
        .otherwise(0))
)




# ============================================================================
# ÉTAPE 4 : ANALYSE PAR ENTREPRISE (TABLE GOLD)
# ============================================================================
# Objectif : Benchmark des entreprises sur les indicateurs qualité
# Décision : Identifier les leaders et les retardataires

company_summary = (df_analysis
    .groupBy("entreprise")
    .agg(
        count("*").alias("total_plaintes"),
        sum("reponse_qualite").alias("reponses_delai_respecte"),
        sum("a_conteste").alias("total_contestations"),
        round(avg("reponse_qualite") * 100, 2).alias("taux_conformite_%"),
        round(avg("a_conteste") * 100, 2).alias("taux_contestation_%"),
        round(avg("delai_reponse"), 1).alias("delai_moyen_jours"),
        round(sum("score_qualite"), 2).alias("score_global")
    )
    .filter(col("total_plaintes") >= 10)  # Seuil de significativité statistique
    .orderBy(col("total_plaintes").desc())
)




# ============================================================================
# ÉTAPE 5 : DÉTECTION DES ENTREPRISES PROBLÉMATIQUES
# ============================================================================
# Objectif : Alerting automatique sur les mauvaises performances
# Décision : Priorisation des inspections et audits

problematic_companies = (company_summary
    .filter((col("taux_conformite_%") < 50) | (col("taux_contestation_%") > 30))
    .select(
        "entreprise",
        "total_plaintes",
        "taux_conformite_%",
        "taux_contestation_%",
        "delai_moyen_jours"
    )
    .orderBy(col("taux_conformite_%").asc())
)

print("=== ENTREPRISES NÉCESSITANT UNE ATTENTION PRIORITAIRE ===")
display(problematic_companies)




# ============================================================================
# ÉTAPE 6 : ANALYSE PAR PRODUIT (RISQUE MÉTIER)
# ============================================================================
# Objectif : Identifier les produits financiers les plus litigieux
# Décision : Renforcement de la régulation sur certains produits

top_problematic_products = (df_analysis
    .groupBy("produit")
    .agg(
        count("*").alias("total_plaintes"),
        round((sum(when(col("conteste") == "Oui", 1).otherwise(0)) / count("*")) * 100, 2).alias("taux_contestation_%"),
        round((sum(when(col("reponse_delai") == "Non", 1).otherwise(0)) / count("*")) * 100, 2).alias("taux_hors_delai_%"),
        round(avg("delai_reponse"), 1).alias("delai_moyen_jours")
    )
    .filter(col("total_plaintes") >= 50)
    .orderBy(col("taux_contestation_%").desc())
    .limit(10)
)





# ============================================================================
# ÉTAPE 7 : ANALYSE TEMPORELLE (TENDANCES)
# ============================================================================
# Objectif : Détecter les variations saisonnières et les pics d'activité
# Décision : Allocation des ressources d'inspection

df_temporal = (df_analysis
    .withColumn("mois_reception", month(to_date(col("date_reception"))))
    .withColumn("annee_reception", year(to_date(col("date_reception"))))
    .groupBy("annee_reception", "mois_reception")
    .agg(
        count("*").alias("nombre_plaintes"),
        round(avg("reponse_qualite") * 100, 2).alias("taux_conformite")
    )
    .orderBy("annee_reception", "mois_reception")
)








# ============================================================================
# ÉTAPE 8 : PERSISTANCE DES DONNÉES (TABLE GOLD)
# ============================================================================
# Objectif : Rendre les résultats accessibles pour reporting et BI
# Enjeu : Créer une source unique de vérité pour l'entreprise

output_path = "/Volumes/complaints/my_schema/complaints_csv/complaints_summary"
company_summary.write.mode("overwrite").parquet(output_path)
company_summary.createOrReplaceTempView("company_summary")







# ============================================================================
# ÉTAPE 9 : RAPPORT DE SYNTHÈSE DÉCISIONNEL
# ============================================================================
# Objectif : Fournir des KPIs agrégés pour le comité de direction
# Décision : Vision macro de la qualité de service du secteur

print("\n" + "="*80)
print("RAPPORT DE SYNTHÈSE - QUALITÉ SERVICE CLIENT SECTEUR FINANCIER")
print("="*80)

total_complaints = df_analysis.count()
companies_count = company_summary.count()

print(f"PLAINTES ANALYSÉES : {total_complaints:,}")
print(f"ENTREPRISES CONCERNÉES : {companies_count}")
print(f"CONFORMITÉ DÉLAIS : {df_analysis.filter(col('reponse_delai') == 'Oui').count() / total_complaints * 100:.1f}%")
print(f"TAUX DE CONTESTATION : {df_analysis.filter(col('conteste') == 'Oui').count() / total_complaints * 100:.1f}%")
print(f"DÉLAI MOYEN RÉPONSE : {df_analysis.select(round(avg('delai_reponse'), 1)).collect()[0][0]} jours")

print("\n" + "-"*80)
print("TOP 5 ENTREPRISES À AUDITER PRIORITAIREMENT")
print("-"*80)
display(company_summary.select("entreprise", "total_plaintes", "taux_conformite_%", "taux_contestation_%").limit(5))

print("Pipeline terminé - Données prêtes pour tableau de bord décisionnel")
