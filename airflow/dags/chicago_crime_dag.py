"""
DAG principal : pipeline Chicago Crime.

S'exécute tous les jours à 6h UTC et traite la journée d'il y a
`data_delay_days` jours (l'API Chicago publie les données avec ~8 jours de retard).

Flux :
  1. extract_api   → API → fichiers locaux → GCS RAW (scripts/upload_to_gcs.py)
  2. spark_bronze  → PySpark : RAW → BRONZE
  3. spark_silver  → PySpark : BRONZE → SILVER (partition de la date, overwrite)
  4. data_quality  → Vérifications sur Silver
  5. load_bigquery → GCS Silver → BigQuery (MERGE sur id, sans doublons)
  6. dbt_run       → dbt models (marts)
  7. dbt_test      → dbt tests
  8. notify        → Notification succès

En cas d'échec définitif d'une tâche : alerte Slack (airflow/plugins/slack_alerts.py).
L'historique (2001 → début du DAG) est chargé à part, par le backfill manuel.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from slack_alerts import send_slack_alert
from pipeline_metrics import record_pipeline_run

# Journée traitée par chaque run : ds - data_delay_days (réglé dans params plus bas)
DATA_DATE = "{{ macros.ds_add(ds, -params.data_delay_days) }}"
DATA_DATE_END = "{{ macros.ds_add(ds, 1 - params.data_delay_days) }}"


# ----- Configuration du DAG -----
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,                          # 3 retries en cas d'échec
    "retry_delay": timedelta(minutes=5),   # 5 min entre retries
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": send_slack_alert,  # alerte Slack après le dernier retry
}

with DAG(
    dag_id="chicago_crime_daily_pipeline",
    default_args=default_args,
    description="Chicago Crime daily ETL pipeline",
    schedule_interval="0 6 * * *",          # Cron : tous les jours à 6h UTC
    start_date=datetime(2026, 11, 24),      # L'historique est chargé par le backfill
    catchup=True,                           # Rattrape automatiquement les jours manqués
    max_active_runs=1,                      # 1 exécution à la fois
    on_success_callback=record_pipeline_run,   # enregistre le run dans silver.pipeline_runs
    on_failure_callback=record_pipeline_run,
    params={
        # Retard de publication de l'API : chaque run traite ds - 10 jours
        "data_delay_days": Param(10, type="integer", minimum=1),
    },
    tags=["chicago", "crime", "production"],
) as dag:

    # ----- 1. Extraction API → GCS RAW -----
    extract_api = BashOperator(
        task_id="extract_api_to_gcs",
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m ingestion.main --start-date {DATA_DATE} --end-date {DATA_DATE_END} && "
            f"python -m scripts.upload_to_gcs {DATA_DATE}"
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

    # ----- 5. Load GCS Silver → BigQuery (MERGE) -----
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
        retries=0,  # relancer des tests en échec ne change pas le résultat
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