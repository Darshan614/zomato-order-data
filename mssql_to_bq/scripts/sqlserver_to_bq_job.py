import os
import sys
import argparse
from pyspark.sql.functions import col, expr, first, sum as Fsum, row_number
from pyspark.sql.window import Window
import subprocess
import inspect
# from dependencies import spark
# print(spark)
os.system('')
os.system('pip list')

for path in sys.path:
    print(path)

print("\n--- Python Interpreter Path ---")
print(sys.executable)

def get_installed_packages():
    try:
        command = [sys.executable, "-m", "pip", "list", "--format", "freeze"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Error running pip list: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return []

print("\n--- Pip Installed Packages ---")
installed_packages = get_installed_packages()
if installed_packages:
    for pkg in installed_packages:
        print(pkg)
else:
    print("Could not retrieve installed packages.")

print("\n--- End Environment Details ---")

try:
    import dependencies
    print(f"Module 'dependencies' found at: {dependencies.__file__}")

    # Optionally, list contents of the 'dependencies' directory if it's not zipped
    # This might only work if it's extracted as a directory, not directly from a zip
    if os.path.isdir(os.path.dirname(dependencies.__file__)):
        print(f"Contents of 'dependencies' directory:")
        for item in os.listdir(os.path.dirname(dependencies.__file__)):
            print(f"  - {item}")
except ImportError as e:
    print(f"ERROR: Could not import 'dependencies': {e}")

from dependencies.spark import start_spark
from google.cloud import secretmanager

def check_loaded_jars(spark_session: SparkSession):
    """Prints the list of JARs loaded by the SparkContext."""
    try:
        # Access the underlying Java SparkContext
        jsc = spark_session.sparkContext._jsc.sc()
        java_jars = jsc.listJars() # This returns a Java Array object

        print("\n--- Spark Loaded JARs ---")
        if java_jars:
            # Iterate through the Java array to get the JAR paths
            for i in range(java_jars.length()):
                print(f"  - {java_jars.apply(i)}")
        else:
            print("  No additional JARs explicitly listed by SparkContext.listJars().")
        print("---------------------------\n")

    except Exception as e:
        print(f"ERROR checking loaded JARs: {e}")

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
        print(f"Accessing secrets for environment: {env} in project: {project_id}")

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
            "bq_dataset_name": bq_dataset_name,
            "bq_temp_gcs_bucket": bq_temp_gcs_bucket,
            "gcp_project_id": gcp_project_id
        }
    
def get_db_config(env, secrets=None):
    if secrets is None:
        secrets = access_secrets(env)
    # jdbc_url = f"jdbc:sqlserver://{secrets['host']}:1433;databaseName={secrets['database']};encrypt=false;trustServerCertificate=true"
    jdbc_url = f"jdbc:sqlserver://{secrets['host']}:1433;databaseName={secrets['database']};encrypt=false;"
    connection_properties = {
        "user": secrets["user"],
        "password": secrets["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, connection_properties

def check_db_connection(spark,log, env, secrets):

    jdbc_url, connection_properties = get_db_config(env, secrets)
    log.info("Connecting to database...")
    log.info(f"JDBC URL: {jdbc_url}")
    log.info(f"Connection Properties: {connection_properties}")

    try:
        df = spark.read.jdbc(jdbc_url,"food.AllFoodItems",properties=connection_properties)
        log.info("Connection Successfull")
    except Exception as e:
        log.error("Connection Failed")
        log.error(e)

def main():
    args = get_args()
    env = args.env
    gcp_project_id = args.gcp_project_id
    bq_dataset_name = args.bq_dataset_name
    bq_temp_gcs_bucket = args.bq_temp_gcs_bucket
    #get all secrets
   
    secrets = access_secrets(env, gcp_project_id, bq_dataset_name, bq_temp_gcs_bucket)
    print(secrets)
    print("starting spark")
    print(inspect.getsource(start_spark))
    spark, log = start_spark(secrets=secrets)
    log.info("starting spark done")
    check_loaded_jars(spark)
    log.info('Session Created, Starting ETL job')
    
    #Extract
    log.info("------------EXTRACT STARTED----------")
    food_items_df, ordered_food_df, orders_df = extract_all_data(spark, log, env, secrets)
    log.info("------------EXTRACT ENDED----------")
    
    #Transform
    log.info("------------TRANSFORM STARTED----------")
    ordered_food_city = join_df(food_items_df, ordered_food_df, orders_df) #joining all tables 
    order_data_filtered = column_filter(ordered_food_city,food_items_df, ordered_food_df) #fetching only required columns
    order_data_filtered = drop_nulls(order_data_filtered) #dropping nulls post join
    order_data_running_city = running_city(order_data_filtered) #calculating running bill_amount for each city with time and orders
    log.info("------------TRANSFORM ENDED----------")

    #Load
    log.info("------------LOAD STARTED_---------")
    load_to_bq(order_data_running_city, log, secrets)    
    log.info("------------LOAD ENDED----------")
    return 

def join_df(food_items_df, ordered_food_df, orders_df):
    ordered_food_df = ordered_food_df.join(food_items_df, ordered_food_df.ItemID == food_items_df.ItemID\
                    ,how="inner")
    ordered_food_city = ordered_food_df.join(orders_df, \
                    ordered_food_df.OrderID==orders_df.OrderID \
                    ,how='inner')
    return ordered_food_city

def column_filter(ordered_food_city, food_items_df, ordered_food_df):
    ordered_food_city = ordered_food_city.select(ordered_food_df['OrderID'].alias('order_id'),food_items_df['ItemID'].alias('item_id')
                                             ,col('Quantity').alias('quantity'),col('ItemName').alias('item_name')
                                             ,col('Price').alias('price'),col('City').alias('city'),col('OrderPlacedAt').alias('order_placed_at')
                                             ,col('billAmount').alias('bill_amount'),col('RiderWaitTime').alias('rider_wait_time')
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
    running_window = Window.partitionBy("city").orderBy("order_placed_at").rowsBetween(Window.unboundedPreceding,
                                                                                    Window.currentRow)
    df = df.withColumn("running_total_city",Fsum("unique_bill_amount").over(running_window))
    df.limit(5).toPandas()
    return df

def extract_all_data(spark, log, env, secrets):
    check_db_connection(spark, log, env, secrets)
    jdbc_url, connection_properties = get_db_config(env, secrets)

    food_items_df = spark.read.jdbc(jdbc_url, 'food.AllFoodItems', properties=connection_properties)
    ordered_food_df = spark.read.jdbc(jdbc_url, 'ord.ordered_items', properties=connection_properties)
    orders_df = spark.read.jdbc(jdbc_url, 'ord.zomato_orders', properties=connection_properties)

    food_tems_count = food_items_df.count()
    ordered_food_count = ordered_food_df.count()
    orders_count = orders_df.count()
    
    log.info("---------DATA LOAD DONE -------------")
    log.info(str(food_tems_count)+" rows loaded for food_items_df")
    log.info(str(ordered_food_count)+" rows loaded for ordered_food_df")
    log.info(str(orders_count)+" rows loaded for orders_df")

    return food_items_df, ordered_food_df, orders_df

def load_to_bq(order_data_running_city, log, secrets):
    try:
        project_id = secrets["gcp_project_id"]
        dataset_name = secrets["bq_dataset_name"]
        temp_gcs_bucket = secrets["bq_temp_gcs_bucket"]

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