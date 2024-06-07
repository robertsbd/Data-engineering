import os

import delta
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc

BRONZE_LAYER = "/Users/benjamin/Documents/Data and AI/delta_lake/storage/bronze/"
SILVER_LAYER = "/Users/benjamin/Documents/Data and AI/delta_lake/storage/silver/"
GOLD_LAYER = "/Users/benjamin/Documents/Data and AI/delta_lake/storage/gold/"


def get_spark() -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("TestApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .enableHiveSupport()
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    spark = get_spark()

    # Get list of all tables
    tbls = spark.catalog.listTables()

    # Read csv files from bronze and write as a delta table in silver and to metastore
    for t in tbls :
        print("Table =  " + t.name)
        cols = spark.sql(f"SHOW columns in default.{t.name}")
        print(cols._jdf.showString(20,20,False))
        print("\n")

if __name__ == "__main__":
    main()
