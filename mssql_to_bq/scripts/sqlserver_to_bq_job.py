from dependencies.spark import start_spark
import os
from pyspark.sql.functions import col,expr,first, sum as Fsum, row_number
from pyspark.sql.window import Window

def main():
    print("starting spark")
    spark, log = start_spark()
    log.info("starting spark done")

    log.info('Session Created, Starting ETL job')

    #Extract
    log.info("------------EXTRACT STARTED----------")
    food_items_df, ordered_food_df, orders_df = extract_all_data(spark, log)
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
    load_to_bq(order_data_running_city, log)    
    log.info("------------LOAD ENDED----------")

def get_db_config():
    jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=Zomato;encrypt=true;trustServerCertificate=true"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, connection_properties

def check_db_connection(spark,log):
    jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=Zomato;encrypt=true;trustServerCertificate=true"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    try:
        df = spark.read.jdbc(jdbc_url,"food.AllFoodItems",properties=connection_properties)
        log.info("Connection Successfull")
    except Exception as e:
        log.error("Connection Failed")
        log.error(e)

    jdbc_url, connection_properties = get_db_config()
    try:
        df = spark.read.jdbc(jdbc_url, "food.AllFoodItems", properties=connection_properties)
        df.show(5)
        log.warn("Connection Successfull")
    except Exception as e:
        print("Connection Failed")
        print(e)

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

def extract_all_data(spark, log):

    check_db_connection(spark, log)
    jdbc_url, connection_properties = get_db_config()

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

def load_to_bq(order_data_running_city, log):
    try:
        order_data_running_city.write\
        .format('bigquery')\
        .option('table','zomato-462103.zomato_orders.zomato_orders_trans')\
        .option('temporaryGcsBucket','zomato-462103-temp')\
        .option("project", "zomato-462103")\
        .mode('append')\
        .save()
        log.info('-------------Data seccussfully written to BigQuery------------')
    except Exception as e:
        log.warn('-------------Failed to write to bigquery-------------')
        log.error(e)


if __name__ == '__main__':
    main()