"""Schemas explicites pour PySpark."""
from pyspark.sql.types import *

# Schéma Bronze : exactement ce que renvoie l'API Socrata

bronze_schema = StructType([
    # ---- Colonnes techniques Socrata ----
    StructField(":id", StringType(), True),
    StructField(":version", StringType(), True),
    StructField(":created_at", TimestampType(), True),
    StructField(":updated_at", TimestampType(), True),
    
    # ---- Colonnes métier ----
    StructField("id", IntegerType(), False),        # Clé primaire, non-null
    StructField("case_number", StringType(), True),
    StructField("date", TimestampType(), False),     # Non-null
    StructField("block", StringType(), True),
    StructField("iucr", StringType(), True),
    StructField("primary_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("arrest", BooleanType(), True),
    StructField("domestic", BooleanType(), True),
    StructField("beat", IntegerType(), True),
    StructField("district", IntegerType(), True),
    StructField("ward", IntegerType(), True),
    StructField("community_area", IntegerType(), True),
    StructField("fbi_code", StringType(), True),
    StructField("x_coordinate", IntegerType(), True),
    StructField("y_coordinate", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("updated_on", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location", StringType(), True),
    
    # ---- Colonnes géographiques calculées par Socrata ----
    # Ces colonnes sont utilisées par le portail Socrata pour les cartes
    # On les garde en Bronze pour audit, mais on les supprimera en Silver
    StructField(":@computed_region_awaf_s7ux", IntegerType(), True),
    StructField(":@computed_region_6mkv_f3dw", IntegerType(), True),
    StructField(":@computed_region_vrxf_vc4k", IntegerType(), True),
    StructField(":@computed_region_bdys_3d7i", IntegerType(), True),
    StructField(":@computed_region_43wa_7qmu", IntegerType(), True),
    StructField(":@computed_region_rpca_8um6", IntegerType(), True),
    StructField(":@computed_region_d9mm_jgwp", IntegerType(), True),
    StructField(":@computed_region_d3ds_rm58", IntegerType(), True),
    StructField(":@computed_region_8hcu_yrd4", IntegerType(), True)
])