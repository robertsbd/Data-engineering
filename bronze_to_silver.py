import os

import delta
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc

# First let's just get this working off a metastore set up in the streamlit app and then we can work from there

BRONZE_LAYER = "/home/benjamin/Documents/data_and_ai/delta_lake/storage/bronze/"
SILVER_LAYER = "/home/benjamin/Documents/data_and_ai/delta_lake/storage/silver/"
GOLD_LAYER = "/home/benjamin/Documents/data_and_ai/delta_lake/storage/gold/"

def get_spark() -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("TestApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
#        .config("spark.sql.warehouse.dir", "/home/benjamin/Documents/data_and_ai/delta_lake/spark-warehouse")
#        .config("hive.metastore.warehouse.dir", "/home/benjamin/Documents/data_and_ai/delta_lake/hive-metastore")
        .enableHiveSupport()
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def main():
    spark = get_spark()

    bronze_files = os.listdir(BRONZE_LAYER) # Get a list of all files in bronze layer

    # First just be able to query our tables from a script in streamlit
#    spark.sql("CREATE DATABASE IF NOT EXISTS hive_test")
    
    # Read csv files from bronze and write as a delta table in silver and to metastore
    for f in bronze_files :
        print(f"Reading from bronze file {f}")
        df = spark.read.option("header", True).csv(BRONZE_LAYER + f)
        silver_file_name = os.path.splitext(f)[0]
        print(f"Writing to silver {f}")
        df.write.format("delta").mode("overwrite").option("path", SILVER_LAYER + silver_file_name).saveAsTable(silver_file_name)
        print(f"File written successfully: {silver_file_name}") # this is not telling me if it has beeen written successfully
        
if __name__ == "__main__":
    main()
