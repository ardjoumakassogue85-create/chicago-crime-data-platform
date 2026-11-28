"""
Alertes Slack pour les échecs du pipeline.

Utilisé comme on_failure_callback dans le DAG :
    from slack_alerts import send_slack_alert

Le dossier airflow/plugins/ est dans le chemin Python d'Airflow,
donc le DAG peut importer ce fichier directement.

Le webhook vient de la variable SLACK_WEBHOOK_URL (fichier .env du projet,
chargé dans les conteneurs via env_file).
"""
import logging
import os

import requests
from airflow.macros import ds_add

log = logging.getLogger(__name__)


def send_slack_alert(context):
    """Envoie une alerte Slack quand une tâche échoue définitivement (après tous ses retries)."""
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        log.warning("SLACK_WEBHOOK_URL non définie : alerte Slack ignorée")
        return

    ti = context["task_instance"]
    ds = context.get("ds")

    # Journée de données traitée par ce run : ds - data_delay_days (params du DAG)
    delay = context.get("params", {}).get("data_delay_days")
    data_date = ds_add(ds, -int(delay)) if ds and delay is not None else "inconnue"

    error = str(context.get("exception") or "voir les logs")[:500]

    text = (
        ":rotating_light: *Pipeline Chicago Crime en échec*\n"
        f"*DAG* : {ti.dag_id}\n"
        f"*Tâche* : {ti.task_id}\n"
        f"*Run* : {ti.run_id}\n"
        f"*Données du* : {data_date}\n"
        f"*Erreur* : {error}\n"
        f"*Logs* : {ti.log_url}"
    )

    try:
        response = requests.post(webhook, json={"text": text}, timeout=10)
        response.raise_for_status()
        log.info("Alerte Slack envoyée")
    except Exception as exc:  # une alerte ratée ne doit jamais faire planter Airflow
        log.error("Échec de l'envoi de l'alerte Slack : %s", exc)