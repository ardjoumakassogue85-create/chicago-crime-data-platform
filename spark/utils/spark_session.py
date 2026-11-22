"""Factory pour créer une SparkSession configurée pour GCP."""
import os
from pyspark.sql import SparkSession
from loguru import logger


def get_spark(app_name: str = "ChicagoCrime") -> SparkSession:
    """
    Crée une SparkSession configurée pour accéder à GCS.
    
    Configuration :
    - Adaptive Query Execution (AQE) : Spark ajuste dynamiquement le plan
    - Shuffle partitions : nombre de partitions après un shuffle
    - Authentification GCS via clé JSON
    """
    
    # Chemin de la clé GCP (depuis .env)
    sa_key_path = os.getenv("GCP_SA_KEY_PATH", "")
    gcs_jar = os.getenv(
    "GCS_CONNECTOR_JAR",
    os.path.expanduser("~/jars/gcs-connector-hadoop3-2.2.5-shaded.jar"),
)
    # Construction de la session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Local : utilise tous les coeurs
        # ----- Adaptive Query Execution -----
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ----- Shuffle -----
        .config("spark.sql.shuffle.partitions", "200")
        # ----- Memory -----
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # ----- GCS Connector (JAR depuis Maven) -----
        .config("spark.jars", gcs_jar)
        # ----- GCS Authentication -----
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.enable",
            "true"
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            sa_key_path
        )
        # ----- Logging -----
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    
    # Réduit le verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"SparkSession créée : {app_name}")
    return spark