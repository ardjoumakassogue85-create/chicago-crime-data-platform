import os
from google.cloud import bigquery
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

def load_silver_to_bg(
    bucket_name: str,
    project_id: str,
    dataset: str = "silver",
    table: str = "crimes"
):

    """
        Charge tous les fichiers Parquet Silver de GCS vers BigQuery.
        
        Optimisations BigQuery :
        - Partitionnement par date (crime_date) : réduit le coût des requêtes par date
        - Clustering par primary_type et district : optimise les filtres
        """
    # Auth via la clé
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_SA_KEY_PATH")
    
    #Client Bigquery
    
    #ID complet de la table : project.dataset.table
    table_id = f"{project_id}.{dataset}.{table}"
    
    #Configuration du job
    job_config = bigquery.LoadJobConfig(
        # Format source : Parquet (BigQuery le lit nativement)
        source_format=bigquery.SourceFormat.PARQUET,
        # Mode d'écriture : APPEND (ajoute aux données existantes)
        # Pour la première charge, BigQuery crée la table
        write_disposition= bigquery.WriteDisposition.WRITE_APPEND,
         # PARTITIONNEMENT : découpe la table par jour sur la colonne crime_date
        # Une requête filtrant sur 1 jour ne scannera que 1/365 du volume
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="crime_date",
    ),

        # CLUSTERING : trie physiquement les données par ces colonnes
        # Idéal pour les colonnes souvent dans la clause WHERE
        clustering_fields=["primary_type", "district"],
    )
    
    ## URL GCS (le ** dit de lire tous les sous-dossiers, ex: crime_year=2023/, crime_year=2024/, etc.)
    uri = f"gs://{bucket_name}/silver/crimes/**/*.parquet"
    
    logger.info(f"📥 Source : {uri}")
    logger.info(f"📤 Destination : {table_id}")
    logger.info("⚙️  Configuration :")
    logger.info("   - Format : Parquet")
    logger.info("   - Mode : APPEND")
    logger.info("   - Partition : DAY sur crime_date")
    logger.info("   - Cluster : primary_type, district")

    #Lancer le job
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
        location="europe-west1",
    )
    
    # .result() bloque jusqu'à la fin du job
    logger.info(f"Lancement du job de chargement: {load_job.job_id}")
    
    #Attendre la fin du job
    load_job.result()
    
    #Verification
    logger.success(f"Chargement terminé! {load_job.output_rows} lignes chargées dans {table_id}")
    table_obj = client.get_table(table_id)
    logger.info(f"✅ Chargé {table_obj.num_rows:,} rows into {table_id}")
    logger.info(f"   Partitioning : {table_obj.partitioning_type}")
    logger.info(f"   Clustering   : {table_obj.clustering_fields}")

    cat > bigquery/load_silver.py << 'PYEOF'
"""Charge les données Silver Parquet depuis GCS vers BigQuery."""
import os
from google.cloud import bigquery
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def load_silver_to_bq(
    bucket_name: str,
    project_id: str,
    dataset: str = "silver",
    table: str = "crimes"
):
    """
    Charge tous les fichiers Parquet Silver de GCS vers BigQuery.
    
    Optimisations BigQuery :
    - Partitionnement par date (crime_date) : réduit le coût des requêtes par date
    - Clustering par primary_type et district : optimise les filtres
    """
    # Auth via la clé
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_SA_KEY_PATH")
    
    # Client BigQuery
    client = bigquery.Client(project=project_id)
    
    # ID complet de la table : project.dataset.table
    table_id = f"{project_id}.{dataset}.{table}"
    
    # Configuration du job de chargement
    job_config = bigquery.LoadJobConfig(
        # Format source : Parquet (BigQuery le lit nativement)
        source_format=bigquery.SourceFormat.PARQUET,
        
        # Mode d'écriture : APPEND (ajoute aux données existantes)
        # Pour la première charge, BigQuery crée la table
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        
        # PARTITIONNEMENT : découpe la table par jour sur la colonne crime_date
        # Une requête filtrant sur 1 jour ne scannera que 1/365 du volume
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="crime_date",
        ),
        
        # CLUSTERING : trie physiquement les données par ces colonnes
        # Idéal pour les colonnes souvent dans la clause WHERE
        clustering_fields=["primary_type", "district"],
    )
    
    # URL GCS (le ** dit de lire tous les sous-dossiers, ex: crime_year=2023/, crime_year=2024/, etc.)
    uri = f"gs://{bucket_name}/silver/crimes/crime_year=*"
    
    logger.info(f"📥 Source : {uri}")
    logger.info(f"📤 Destination : {table_id}")
    logger.info("⚙️  Configuration :")
    logger.info("   - Format : Parquet")
    logger.info("   - Mode : APPEND")
    logger.info("   - Partition : DAY sur crime_date")
    logger.info("   - Cluster : primary_type, district")
    
    # Lancement du job (asynchrone)
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
        location="europe-west1",
    )
    
    # .result() bloque jusqu'à la fin du job
    logger.info("⏳ Chargement en cours...")
    load_job.result()
    
    # Vérification
    table_obj = client.get_table(table_id)
    logger.info(f"✅ Loaded {table_obj.num_rows:,} rows into {table_id}")
    logger.info(f"   Partitioning : {table_obj.partitioning_type}")
    logger.info(f"   Clustering   : {table_obj.clustering_fields}")


if __name__ == "__main__":
    load_silver_to_bq(
        bucket_name=os.getenv("GCP_BUCKET_NAME"),
        project_id=os.getenv("GCP_PROJECT_ID"),
    )