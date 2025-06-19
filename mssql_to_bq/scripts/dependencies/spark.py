from pyspark.sql import SparkSession
# import pandas as pd
import os
from dependencies import logging

print("function to start spark session")
def start_spark(app_name='sqlserver_to_bq', master='local[*]', jar_packages=[], files=[], spark_config={},secrets=None):
    spark = SparkSession.builder \
    .appName(app_name) \
    .master(master) \
    .config("parentProject", secrets.gcp_project_id) \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json", secrets["dproc_sa_key"]) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()

    spark_logger = logging.Log4j(spark)
    return spark, spark_logger