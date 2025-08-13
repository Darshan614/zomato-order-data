import os
import sys
import argparse
from pyspark.sql.functions import col, expr, first, sum as Fsum, row_number
from pyspark.sql.window import Window
from pyspark import StorageLevel
from pyspark.sql.functions import broadcast
import subprocess
import inspect
from dependencies.spark import start_spark
from google.cloud import secretmanager


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=str, default='on-prem', help='Environment: on-prem, dev, stg, prod')
    parser.add_argument("--gcp-project-id", type=str, required=True,
                    help="The Google Cloud Project ID where secrets and BigQuery resources are located.")
    parser.add_argument("--bq-dataset-name", type=str, required=True,
                        help="The BigQuery dataset name for the target table.")
    parser.add_argument("--bq-temp-gcs-bucket", type=str, required=True,
                        help="The GCS bucket to use for temporary BigQuery data.")
    return parser.parse_args()

def access_secrets(env, gcp_project_id, bq_dataset_name, bq_temp_gcs_bucket):
    # log.info(f"Accessing secrets for environment: {env}")
    if env in ['on-prem', 'dev']:
        if env == 'on-prem':
            return {
                "user": os.getenv("DB_USER_ON_PREM"),
                "password": os.getenv("DB_PASSWORD_ON_PREM"),
                "host": os.getenv("DB_HOST_ON_PREM"),
                "database": os.getenv("DB_NAME_ON_PREM"),
                "bq_dataset_name": os.getenv("BQ_DATASET_DEV"),
                "bq_temp_gcs_bucket": os.getenv("BQ_TEMP_GCS_BUCKET_DEV"),
                "gcp_project_id": os.getenv("GCP_PROJECT_DEV")
            }
        else:  # dev
            return {
                "user": os.getenv("DB_USER_DEV"),
                "password": os.getenv("DB_PASSWORD_DEV"),
                "host": os.getenv("DB_HOST_DEV"),
                "database": os.getenv("DB_NAME_DEV"),
                "bq_dataset_name": os.getenv("BQ_DATASET_DEV"),
                "bq_temp_gcs_bucket": os.getenv("BQ_TEMP_GCS_BUCKET_DEV"),
                "gcp_project_id": os.getenv("GCP_PROJECT_DEV")
            }
    elif env in ['stg', 'prod']:
        client = secretmanager.SecretManagerServiceClient()
        project_id = gcp_project_id

        def get_secret(secret_id):
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode('UTF-8')

        return {
            "user": get_secret(f"{env}-db-user"),
            "password": get_secret(f"{env}-db-password"),
            "host": get_secret(f"{env}-db-host"),
            "database": get_secret(f"{env}-db-name"),
            "dproc_sa_key" : get_secret(f"{env}-dproc-sa"),
            "connection_name" : get_secret(f"{env}-connection-name"),
            "bq_dataset_name": bq_dataset_name,
            "bq_temp_gcs_bucket": bq_temp_gcs_bucket,
            "gcp_project_id": gcp_project_id
        }

def main():
    args = get_args()
    env = args.env
    gcp_project_id = args.gcp_project_id
    bq_dataset_name = args.bq_dataset_name
    bq_temp_gcs_bucket = args.bq_temp_gcs_bucket
    #get all secrets
   
    secrets = access_secrets(env, gcp_project_id, bq_dataset_name, bq_temp_gcs_bucket)
    spark, log = start_spark(secrets=secrets)
    log.info("starting spark done")
    # check_loaded_jars(spark)
    log.info('Session Created, Starting ETL job')
    
    #Extract
    log.info("------------EXTRACT STARTS----------")
    food_items_df, ordered_food_df, orders_df = extract_all_data(spark, log, env, secrets)
    log.info("------------EXTRACT ENDS----------")
    
    #Transform
    log.info("------------TRANSFORM STARTED----------")
    ordered_food_city = join_df(food_items_df, ordered_food_df, orders_df) #joining all tables 
    order_data_filtered = column_filter(ordered_food_city,food_items_df, ordered_food_df) #fetching only required columns
    order_data_filtered = drop_nulls(order_data_filtered) #dropping nulls post join
    order_data_running_city = running_city(order_data_filtered) #calculating running bill_amount for each city with time and orders
    log.info("------------TRANSFORM ENDS----------")

    #Load
    log.info("------------BQ LOAD STARTED_---------")
    load_to_bq(order_data_running_city, log, secrets)    
    log.info("------------BQ LOAD ENDS----------")
    return 

def join_df(food_items_df, ordered_food_df, orders_df):
    ordered_food_df = ordered_food_df.join(broadcast(food_items_df), ordered_food_df.ItemID == food_items_df.ItemID\
                    ,how="inner")
    ordered_food_city = ordered_food_df.join(orders_df, \
                    ordered_food_df.OrderID==orders_df.OrderID \
                    ,how='inner')
    return ordered_food_city

def column_filter(ordered_food_city, food_items_df, ordered_food_df):
    ordered_food_city = ordered_food_city.select(ordered_food_df['OrderID'].alias('order_id'),food_items_df['ItemID'].alias('item_id')
                                             ,col('Quantity').alias('quantity'),col('ItemName').alias('item_name')
                                             ,col('Price').alias('price'),col('City').alias('city'),col('OrderPlacedAt').alias('order_placed_at')
                                             ,col('BillAmount').alias('bill_amount'),col('RiderWaitTime').alias('rider_wait_time')
                                             ,col('KPTDuration').alias('kpt_duration'),col('TotalDuration').alias('total_duration'))
    return ordered_food_city

def drop_nulls(order_data_filtered):
    order_data_filtered = order_data_filtered.dropna(subset=["city","order_placed_at"])
    return order_data_filtered

def running_city(order_data_filtered):
    #remove duplicate bill_amount for same order id
    dedup_window = Window.partitionBy('order_id')
    df = order_data_filtered.withColumn("unique_bill_amount",first("bill_amount").over(dedup_window))

    #add row number for each order 
    order_window = Window.partitionBy("order_id").orderBy("item_id")
    df = df.withColumn("row_num",row_number().over(order_window))

    #replace null unique_bill_amount value with 0
    df = df.withColumn("unique_bill_amount",expr("CASE when row_num=1 THEN unique_bill_amount else 0 end"))

    #calculate running total city i.e. revenue over time for each city
    df = df.repartition("city")
    running_window = Window.partitionBy("city").orderBy("order_placed_at").rowsBetween(Window.unboundedPreceding,
                                                                                    Window.currentRow)
    df = df.withColumn("running_total_city",Fsum("unique_bill_amount").over(running_window))
    return df

def extract_all_data(spark, log, env, secrets):
    food_items_df = spark.read.parquet("gs://zomato-parquet-dump/allfooditems/*")\
        .select(
            col("itemid").alias("ItemID"),
            col("itemname").alias("ItemName"),
            col("price").alias("Price")
        )

    # Helper to read nested payload tables
    def read_payload_only(path, rename_map=None):
        df = spark.read.parquet(path).select("payload.*")
        if rename_map:
            for old_col, new_col in rename_map.items():
                df = df.withColumnRenamed(old_col, new_col)
        return df

    # Read ordered items
    ordered_food_df = read_payload_only(
        "gs://zomato-parquet-dump/ord_ordered_items/*",
        rename_map={
            "order_id": "OrderID",
            "item_id": "ItemID",
            "quantity": "Quantity"
        }
    )

    # Read orders
    orders_df = read_payload_only(
        "gs://zomato-parquet-dump/ord_zomato_orders/*",
        rename_map={
            "city": "City",
            "orderid": "OrderID",
            "orderplacedat": "OrderPlacedAt",
            "billamount": "BillAmount",
            "riderwaittime": "RiderWaitTime",
            "kptduration": "KPTDuration",
            "totalduration": "TotalDuration"
        }
    )

    food_items_df = food_items_df.cache()
    ordered_food_df = ordered_food_df.persist(StorageLevel.MEMORY_AND_DISK)
    orders_df = orders_df.persist(StorageLevel.MEMORY_AND_DISK)

    food_items_count = food_items_df.count()
    ordered_food_count = ordered_food_df.count()
    orders_count = orders_df.count()
    
    log.info("---------DATA LOAD DONE -------------")
    log.info(f"{food_items_count} rows loaded for food_items_df")
    log.info(f"{ordered_food_count} rows loaded for ordered_food_df")
    log.info(f"{orders_count} rows loaded for orders_df")

    return food_items_df, ordered_food_df, orders_df

def load_to_bq(order_data_running_city, log, secrets):
    try:
        project_id = secrets["gcp_project_id"]
        dataset_name = secrets["bq_dataset_name"]
        temp_gcs_bucket = secrets["bq_temp_gcs_bucket"]
        order_data_running_city = order_data_running_city.coalesce(10)
        order_data_running_city.write\
        .format('bigquery')\
        .option('table', f'{project_id}.{dataset_name}.zomato_orders_trans')\
        .option('temporaryGcsBucket', temp_gcs_bucket)\
        .option("project", project_id)\
        .mode('append')\
        .save()
        log.info('-------------Data seccussfully written to BigQuery------------')
    except Exception as e:
        log.warn('-------------Failed to write to bigquery-------------')
        log.error(e)

if __name__ == '__main__':
    main()