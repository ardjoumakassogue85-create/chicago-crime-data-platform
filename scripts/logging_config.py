"""
Configuration loguru commune à tous les scripts du pipeline.

Appeler setup_logging() au tout début du bloc `if __name__ == "__main__":`
de chaque script :

    if __name__ == "__main__":
        from scripts.logging_config import setup_logging
        setup_logging()
        ...

- Console : format lisible, capturé par Airflow dans les logs de chaque tâche
- Fichier  : logs/pipeline/pipeline_<date>.log
    · un fichier par jour, gardé 30 jours, compressé en zip
    · dans le conteneur : /opt/airflow/logs/pipeline/
    · sur ton PC        : airflow/logs/pipeline/ (dossier monté)
    · utile surtout pour le backfill manuel, qui ne passe pas par Airflow
"""
import os
import sys

from loguru import logger

LOG_DIR = os.getenv("PIPELINE_LOG_DIR", "logs/pipeline")
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module} | {message}"

_configured = False


def setup_logging(level: str = "INFO") -> None:
    """Configure loguru une seule fois par processus."""
    global _configured
    if _configured:
        return

    logger.remove()  # retire le handler par défaut

    logger.add(sys.stdout, format=LOG_FORMAT, level=level)

    logger.add(
        os.path.join(LOG_DIR, "pipeline_{time:YYYY-MM-DD}.log"),
        format=LOG_FORMAT,
        rotation="00:00",       # nouveau fichier chaque jour à minuit
        retention="30 days",
        compression="zip",
        level="DEBUG",
        enqueue=True,           # sûr si plusieurs processus écrivent en même temps
    )

    _configured = True