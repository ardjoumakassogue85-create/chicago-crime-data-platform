"""
DAG principal : pipeline Chicago Crime.

S'exécute tous les jours à 6h UTC.

Flux :
  1. extract_api   → Python: API → GCS RAW
  2. spark_bronze  → PySpark: RAW → BRONZE
  3. spark_silver  → PySpark: BRONZE → SILVER
  4. data_quality  → Vérifications sur Silver
  5. load_bigquery → GCS Silver → BigQuery
  6. dbt_run       → dbt models (marts)
  7. dbt_test      → dbt tests
  8. notify        → Notification succès
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# L'API Chicago publie les données avec 7 jours de retard :
# chaque run traite donc la journée d'il y a 7 jours.
DATA_DATE = "{{ macros.ds_add(ds, -7) }}"
DATA_DATE_END = "{{ macros.ds_add(ds, -6) }}"
# ----- Configuration du DAG -----
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,                    # 3 retries en cas d'échec
    "retry_delay": timedelta(minutes=5),  # 5 min entre retries
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="chicago_crime_daily_pipeline",
    default_args=default_args,
    description="Chicago Crime daily ETL pipeline",
    schedule_interval="0 6 * * *",  # Cron : tous les jours à 6h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                   # Ne pas rattraper les exécutions passées
    max_active_runs=1,               # 1 exécution à la fois
    tags=["chicago", "crime", "production"],
) as dag:

        # ----- 1. Extraction API → GCS RAW -----
    extract_api = BashOperator(
        task_id="extract_api_to_gcs",
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m ingestion.main "
            f"--start-date {DATA_DATE} "
            f"--end-date {DATA_DATE_END}"
        ),
    )

    # ----- 2. Spark RAW → BRONZE -----
    spark_bronze = BashOperator(
        task_id="spark_raw_to_bronze",
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m spark.jobs.raw_to_bronze {DATA_DATE}"
        ),
    )

    # ----- 3. Spark BRONZE → SILVER -----
    spark_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m spark.jobs.bronze_to_silver {DATA_DATE}"
        ),
    )

    # ----- 4. Data Quality -----
    data_quality = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            "cd /opt/airflow && "
            "python -m data_quality.report"
        ),
    )

    # ----- 5. Load GCS Silver → BigQuery -----
    load_bigquery = BashOperator(
        task_id="load_silver_to_bigquery",
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m bigquery.load_silver {DATA_DATE}"
        ),
    )

    # ----- 6. dbt Run -----
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --target prod"
        ),
    )

    # ----- 7. dbt Test -----
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --target prod"
        ),
    )

    # ----- 8. Notification succès -----
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "✅ Pipeline executé avec succès"',
    )

    # ----- Dépendances -----
    # extract_api → spark_bronze → spark_silver → data_quality
    #            → load_bigquery → dbt_run → dbt_test → notify_success
    (
        extract_api
        >> spark_bronze
        >> spark_silver
        >> data_quality
        >> load_bigquery
        >> dbt_run
        >> dbt_test
        >> notify_success
    )