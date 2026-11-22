"""Valide les données Silver en lisant le Parquet."""
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from spark.utils.spark_session import get_spark

load_dotenv()

def main():
    bucket = os.getenv("GCP_BUCKET_NAME")
    spark = get_spark("validation")
    
    print("=" * 60)
    print("SILVER VALIDATION")
    print("=" * 60)
    
    df = spark.read.parquet(f"gs://{bucket}/silver/crimes/")
    
    print(f"\n📐 Schema:")
    df.printSchema()
    
    total = df.count()
    print(f"\n📊 Lignes totales: {total:,}")
    
    print(f"\n📅 lignes par années:")
    df.groupBy("crime_year").count().orderBy("crime_year").show()
    
    print(f"\n🚔 Top 10 crime types:")
    df.groupBy("primary_type").count().orderBy(F.desc("count")).show(10)
    
    print(f"\n📍 Null coordinates:")
    df.filter(F.col("latitude").isNull()).count()
    
    spark.stop()

if __name__ == "__main__":
    main()