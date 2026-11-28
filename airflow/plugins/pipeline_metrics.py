"""
Enregistre chaque run du DAG dans BigQuery : silver.pipeline_runs.

Utilisé comme callback au niveau du DAG (pas des tâches) :
    DAG(..., on_success_callback=record_pipeline_run,
             on_failure_callback=record_pipeline_run)

Airflow l'appelle une seule fois, quand le run est terminé (succès ou échec).
Idempotent : si un run est relancé (Clear), sa ligne est mise à jour, jamais dupliquée.

Les logs de ce callback sont dans airflow/logs/scheduler/ (pas dans les logs des tâches).
Une erreur ici est seulement journalisée : elle ne fait jamais échouer le pipeline.
"""
import logging
import os
from datetime import datetime, timezone

from airflow.macros import ds_add
from google.cloud import bigquery

log = logging.getLogger(__name__)

FAILED_STATES = {"failed", "upstream_failed"}


def _count_rows_for_date(client: bigquery.Client, project_id: str, data_date: str) -> int:
    """Nombre de crimes présents dans silver.crimes pour la journée traitée."""
    query = f"""
        SELECT COUNT(*) AS n
        FROM `{project_id}.silver.crimes`
        WHERE CAST(crime_date AS DATE) = @data_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("data_date", "DATE", data_date)]
    )
    return list(client.query(query, job_config=job_config).result())[0].n


def record_pipeline_run(context):
    """Écrit (ou met à jour) la ligne du run dans silver.pipeline_runs."""
    try:
        project_id = os.getenv("GCP_PROJECT_ID")
        key_path = os.getenv("GCP_SA_KEY_PATH")

        dag_run = context["dag_run"]
        ds = context["ds"]
        delay = context["params"]["data_delay_days"]
        data_date = ds_add(ds, -int(delay))

        # Statut final du run + tâches en échec
        status = "success" if str(dag_run.state) == "success" else "failed"
        failed_tasks = sorted(
            ti.task_id for ti in dag_run.get_task_instances() if ti.state in FAILED_STATES
        )

        # Durée totale du run
        end = dag_run.end_date or datetime.now(timezone.utc)
        duration = (end - dag_run.start_date).total_seconds() if dag_run.start_date else None

        client = bigquery.Client.from_service_account_json(key_path, project=project_id)

        rows_processed = None
        try:
            rows_processed = _count_rows_for_date(client, project_id, data_date)
        except Exception as exc:
            log.warning("Comptage des lignes impossible : %s", exc)

        merge_sql = f"""
            MERGE `{project_id}.silver.pipeline_runs` T
            USING (
                SELECT
                    @run_id         AS run_id,
                    @execution_date AS execution_date,
                    @data_date      AS data_date,
                    @status         AS status,
                    @failed_tasks   AS failed_tasks,
                    @rows_processed AS rows_processed,
                    @duration       AS processing_duration_seconds,
                    CURRENT_TIMESTAMP() AS recorded_at
            ) S
            ON T.run_id = S.run_id
            WHEN MATCHED THEN UPDATE SET
                execution_date = S.execution_date,
                data_date = S.data_date,
                status = S.status,
                failed_tasks = S.failed_tasks,
                rows_processed = S.rows_processed,
                processing_duration_seconds = S.processing_duration_seconds,
                recorded_at = S.recorded_at
            WHEN NOT MATCHED THEN INSERT ROW
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", dag_run.run_id),
                bigquery.ScalarQueryParameter("execution_date", "DATE", ds),
                bigquery.ScalarQueryParameter("data_date", "DATE", data_date),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter(
                    "failed_tasks", "STRING", ", ".join(failed_tasks) or None
                ),
                bigquery.ScalarQueryParameter("rows_processed", "INT64", rows_processed),
                bigquery.ScalarQueryParameter("duration", "FLOAT64", duration),
            ]
        )
        client.query(merge_sql, job_config=job_config).result()

        log.info(
            "Run %s enregistré : %s, données du %s, %s lignes",
            dag_run.run_id, status, data_date, rows_processed,
        )

    except Exception as exc:  # ne jamais faire échouer le pipeline à cause des métriques
        log.error("Enregistrement du run dans pipeline_runs impossible : %s", exc)