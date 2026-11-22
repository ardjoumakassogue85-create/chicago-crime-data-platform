"""
Job 1 : RAW (JSON) → BRONZE (Parquet typé).

Rôle : appliquer le schéma explicite et convertir en Parquet.
On ne fait AUCUNE transformation métier ici, juste le typage.
"""
import sys
import os
from loguru import logger
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType    

load_dotenv()

from spark.utils.spark_session import get_spark
from spark.utils.schema import bronze_schema

# =====  fonction de lecture, définie AVANT run() =====
def read_raw(spark, input_path: str):
    """Lit le JSON RAW (tableau indenté, nombres en string) et caste vers bronze_schema."""
    raw_schema = StructType(
        [StructField(f.name, StringType(), True) for f in bronze_schema.fields]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    return (
        spark.read.json(
            input_path,
            schema=raw_schema,
            mode="PERMISSIVE",
            multiLine=True,
        )
        .select(
            *[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in bronze_schema.fields],
            "_corrupt_record",
        )
    )
# ============================================================


def run(ingestion_date: str, gcs_bucket: str):
    """
    Transforme les données RAW JSON en Parquet Bronze.
    
    Args:
        ingestion_date: Date d'ingestion (ex: "2024-01-01")
        gcs_bucket: Nom du bucket GCS (ex: "chicago-crime-data-XYZ")
    """
    # 1. Démarrer Spark
    spark = get_spark("raw_to_bronze")
    
    # 2. Construire les chemins
    input_path = f"gs://{gcs_bucket}/raw/crimes/ingestion_date={ingestion_date}/"
    output_path = f"gs://{gcs_bucket}/bronze/crimes/ingestion_date={ingestion_date}/"
    
    logger.info(f"donnée Json prêt de  {input_path}")
    
    # 3. Lecture du JSON avec le schéma explicite
    # - schema : on FORCE le schéma (pas d'inférence)
    # Le JSON Socrata est un tableau indenté avec les nombres entre guillemets :
    # on lit tout en string (multiLine=True), puis on caste vers bronze_schema
    raw_schema = StructType(
        [StructField(f.name, StringType(), True) for f in bronze_schema.fields]
        + [StructField("_corrupt_record", StringType(), True)]
    )

    df = (
        spark.read.json(
            input_path,
            schema=raw_schema,
            mode="PERMISSIVE",
            multiLine=True,
        )
        .select(
            *[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in bronze_schema.fields],
            "_corrupt_record",
        )
        .cache()
    )
    
    # 4. Compter les lignes (action, déclenche le calcul)
    count = df.count()
    valid = df.filter(F.col("id").isNotNull()).count()
    bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.info(f"Bronze : {count:,} lignes | valides : {valid:,} | corrompues : {bad:,}")

    if valid == 0:
        logger.error("Aucune ligne valide (id nul partout) : écriture annulée")
        spark.stop()
        sys.exit(1)

    df = df.drop("_corrupt_record")   # on ne l'écrit pas dans le Parquet
    
    # 5. Écriture en Parquet (format colonne, compressé par défaut)
    logger.info(f"Parquet écrit dans {output_path}")
    (
        df.write
        .mode("overwrite")  # On écrase si le dossier existe déjà
        .parquet(output_path)
    )
    
    logger.info(" Données Parquet écrites avec succès")
    
    # 6. Arrêt propre
    spark.stop()


if __name__ == "__main__":
    # Récupère les arguments
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    gcs_bucket = os.getenv("GCP_BUCKET_NAME")
    
    if not gcs_bucket or gcs_bucket == "chicago-crime-data":
        logger.error(" GCP_BUCKET_NAME n'est pas defini dans .env")
        sys.exit(1)
    
    run(ingestion_date, gcs_bucket)