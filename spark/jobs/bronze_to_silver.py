"""
Job 2 : BRONZE → SILVER (nettoyage + enrichissement).

Lit le Parquet Bronze, applique les transformations métier,
et écrit le résultat en Parquet Silver, partitionné par année.
"""
import sys
import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

from spark.utils.spark_session import get_spark
from spark.utils.cleaning import clean_crimes

def run(ingestion_date: str, gcs_bucket: str):
    """
    Transforme les données Bronze en Silver.
    
    Args:
        ingestion_date: Date d'ingestion
        gcs_bucket: Nom du bucket GCS
    """
    # 1. Démarrer Spark
    spark = get_spark("bronze_to_silver")

    # 2. Chemins
    input_path = f"gs://{gcs_bucket}/bronze/crimes/ingestion_date={ingestion_date}/"
    output_path = f"gs://{gcs_bucket}/silver/crimes/ingestion_date={ingestion_date}/"
    
    logger.info(f"Preparation de bronze depuis {input_path}")

    # 3. Lecture du Parquet Bronze
    df = spark.read.parquet(input_path)

     # 4. Affichage du schéma pour debug
    logger.info("Bronze schema:")
    df.printSchema()

      # 5. Comptage initial
    initial_count = df.count()
    logger.info(f"Nombre de lignes iniatiale de bronze: {initial_count:,}")

    # 6. Application du nettoyage (LAZY : aucune donnée n'est encore traitée)
    df_clean = clean_crimes(df)

    # 7. Comptage après nettoyage (déclenche tout le pipeline)
    final_count = df_clean.count()
    logger.info(f"Nombre de lignes finale de silver après nettoyage: {final_count:,}")
    logger.info(f" Nombre de lignes supprimées: {initial_count - final_count:,}")

    # 8. Écriture en Parquet, partitionné par année
    # - mode="append" : on ajoute aux partitions existantes
    # - partitionBy="crime_year" : crée un sous-dossier par année
    #     /silver/crimes/crime_year=2023/part-00000-xxx.snappy.parquet
    #     /silver/crimes/crime_year=2024/part-00000-xxx.snappy.parquet
    # → Si on filtre sur 2023, Spark ne lit QUE ce dossier (partition pruning)
    logger.info(f" Enregistrement des données nettoyer dans silver (partionner par crime_year) dans {output_path}")
    (
        df_clean.write
        .mode("overwrite")  # remplace uniquement le dossier de ce jour
        .partitionBy("crime_year")
        .parquet(output_path)
    )
    
    logger.info("Enregistrement éffectué")
    spark.stop()

if __name__ == "__main__":
    from scripts.logging_config import setup_logging
    setup_logging()
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    gcs_bucket = os.getenv("GCP_BUCKET_NAME", "chicago-crime-data")
    
    if not gcs_bucket or gcs_bucket == "chicago-crime-data":
        logger.error("GCP_BUCKET_NAME pas dans .env")
        sys.exit(1)
    
    run(ingestion_date, gcs_bucket)
    