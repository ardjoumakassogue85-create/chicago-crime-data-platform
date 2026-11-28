"""
Charge UNE journée de Silver (GCS) dans BigQuery, de façon idempotente.

1. Charge les Parquet de silver/crimes/ingestion_date=<date>/ dans une table de staging
2. Crée la table finale au premier run (partition crime_date, cluster primary_type/district)
3. MERGE sur `id` : met à jour les crimes déjà présents, insère les nouveaux

→ Relancer le même jour ne crée jamais de doublons.

Usage : python -m bigquery.load_silver 2024-01-01
"""
import os
import sys

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger

load_dotenv()

LOCATION = "europe-west1"


def load_silver_to_bq(
    ingestion_date: str,
    bucket_name: str,
    project_id: str,
    dataset: str = "silver",
    table: str = "crimes",
):
    # Auth via la clé
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_SA_KEY_PATH")
    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset}.{table}"
    staging_id = f"{project_id}.{dataset}._staging_{table}_{ingestion_date.replace('-', '')}"
    # "crime_year=*" : ne prend que les fichiers Parquet (exclut le fichier _SUCCESS de Spark)
    uri = f"gs://{bucket_name}/silver/crimes/ingestion_date={ingestion_date}/crime_year=*"

    logger.info(f"📥 Source      : {uri}")
    logger.info(f"🧪 Staging     : {staging_id}")
    logger.info(f"📤 Destination : {table_id}")

    try:
        # 1. Charger la journée dans la table de staging (remplacée à chaque run)
        load_job = client.load_table_from_uri(
            uri,
            staging_id,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
            location=LOCATION,
        )
        load_job.result()
        staging = client.get_table(staging_id)
        logger.info(f"✅ {staging.num_rows:,} lignes chargées en staging")

        # 2. Créer la table finale au premier run, avec le même schéma que le staging
        try:
            client.get_table(table_id)
        except NotFound:
            new_table = bigquery.Table(table_id, schema=staging.schema)
            new_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="crime_date",
            )
            new_table.clustering_fields = ["primary_type", "district"]
            client.create_table(new_table)
            logger.info(f"🆕 Table créée : {table_id}")

        # 3. MERGE sur id
        columns = [field.name for field in staging.schema]
        has_updated_on = "updated_on" in columns
        # Si un id apparaît plusieurs fois dans la journée, on garde la version la plus récente
        order_by = "updated_on DESC" if has_updated_on else "id"
        # On ne met à jour que si la fiche reçue est plus récente que celle en base
        update_condition = "AND S.updated_on > T.updated_on" if has_updated_on else ""
        update_set = ",\n                ".join(
            f"`{col}` = S.`{col}`" for col in columns if col != "id"
        )

        merge_sql = f"""
            MERGE `{table_id}` T
            USING (
                SELECT * FROM `{staging_id}`
                WHERE TRUE
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY {order_by}) = 1
            ) S
            ON T.id = S.id
            WHEN MATCHED {update_condition} THEN UPDATE SET
                {update_set}
            WHEN NOT MATCHED THEN INSERT ROW
        """

        logger.info(f"🔄 MERGE dans {table_id}")
        merge_job = client.query(merge_sql, location=LOCATION)
        merge_job.result()

        stats = merge_job.dml_stats
        if stats is not None:
            logger.info(
                f"   {stats.inserted_row_count:,} insérées, "
                f"{stats.updated_row_count:,} mises à jour"
            )

        table_obj = client.get_table(table_id)
        logger.success(f"📊 Total dans {table_id} : {table_obj.num_rows:,} lignes")

    finally:
        client.delete_table(staging_id, not_found_ok=True)
        logger.info("🧹 Table de staging supprimée")


if __name__ == "__main__":
    from scripts.logging_config import setup_logging
    setup_logging()
    if len(sys.argv) < 2:
        logger.error("Usage : python -m bigquery.load_silver <ingestion_date YYYY-MM-DD>")
        sys.exit(1)

    bucket_name = os.getenv("GCP_BUCKET_NAME")
    project_id = os.getenv("GCP_PROJECT_ID")
    if not bucket_name or not project_id:
        logger.error("GCP_BUCKET_NAME ou GCP_PROJECT_ID manquant dans l'environnement")
        sys.exit(1)

    load_silver_to_bq(sys.argv[1], bucket_name, project_id)