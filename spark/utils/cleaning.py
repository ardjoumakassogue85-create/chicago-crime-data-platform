"""Fonctions de nettoyage pour la couche Silver."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def clean_crimes(df: DataFrame) -> DataFrame:
    """
    Pipeline complet de nettoyage Bronze → Silver.
    
    Étapes :
    1. Déduplication par id
    2. Filtre des id nulls
    3. Renommage de 'date' en 'crime_datetime'
    4. Enrichissement temporel (year, month, day, hour, day_of_week, is_weekend)
    5. Validation des coordonnées (Chicago est aux alentours de 41°N, 87°W)
    6. Normalisation du texte (UPPER + TRIM, "UNKNOWN" si null)
    7. Suppression des colonnes techniques Socrata
    """
    
    # 1. Déduplication : supprime les lignes avec le même 'id'
    # Garde la première occurrence rencontrée
    df = df.dropDuplicates(["id"])

    # 2. Filtre : supprime les lignes où 'id' est null
    # (clé primaire obligatoire)
    df = df.filter(F.col("id").isNotNull())
    
    # 3. Renommage : 'date' est trop générique, on précise 'date' -> 'crime_datetime'   
    df = df.withColumnRenamed("date", "crime_datetime")

    # 4. Enrichissement temporel
    # On extrait des composantes de la date pour permettre des filtres rapides
    df = (
        df
        .withColumn("crime_date", F.to_date("crime_datetime"))
        .withColumn("crime_year", F.year("crime_date"))
        .withColumn("crime_month", F.month("crime_date"))
        .withColumn("crime_day", F.dayofmonth("crime_date"))
        .withColumn("crime_hour", F.hour("crime_datetime"))
        .withColumn("crime_day_of_week", F.dayofweek("crime_date"))
        .withColumn("crime_is_weekend", F.when(F.col("crime_day_of_week").isin([1, 7]), 1).otherwise(0))
   
    )

    # 5. Validation géographique
    # Chicago est grossièrement entre :
    #   Latitude : 41.6 à 42.0
    #   Longitude : -87.9 à -87.5
    # Si une coordonnée est hors de cette plage, c'est une erreur → on met NULL
    df = df.withColumn("latitude", F.when(F.col("latitude").between(41.6, 42.0), F.col("latitude")).otherwise(None))
    df = df.withColumn("longitude", F.when(F.col("longitude").between(-87.9, -87.5), F.col("longitude")).otherwise(None))

    # 6. Normalisation du texte
    # - UPPER : tout en majuscules (uniformité)
    # - TRIM : supprime les espaces inutiles au début/fin
    # - "UNKNOWN" si la valeur est null
    for col_name in ["primary_type", "description", "location_description"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(), "UNKNOWN")
            .otherwise(F.upper(F.trim(F.col(col_name))))
        )
    
    # 7. Suppression des colonnes techniques Socrata
    # Ces colonnes sont spécifiques au portail Socrata et inutiles pour l'analyse
    cols_to_drop = [
        # Métadonnées Socrata
        ":id", ":version", ":created_at", ":updated_at",
        # Coordonnées State Plane (obsolètes, on utilise lat/lon)
        "x_coordinate", "y_coordinate",
        # Format string redondant avec latitude/longitude
        "location",
        # Codes géographiques Socrata
        ":@computed_region_awaf_s7ux",
        ":@computed_region_6mkv_f3dw",
        ":@computed_region_vrxf_vc4k",
        ":@computed_region_bdys_3d7i",
        ":@computed_region_43wa_7qmu",
        ":@computed_region_rpca_8um6",
        ":@computed_region_d9mm_jgwp",
        ":@computed_region_d3ds_rm58",
        ":@computed_region_8hcu_yrd4"
    ]
    df = df.drop(*cols_to_drop)
    
    return df