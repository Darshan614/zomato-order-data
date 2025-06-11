# zomato-order-data

This project contains ETL pipeline to fetch data of zomato orders from OLTP to OLAP. It fetches data from 3 tables containg data of food, order item and order data.

![ER Diagram](/mssql_to_bq/images/Zomato_Physical_ER.png)
 
 
 It comprises of three environments depicting real world developement of spark jobs(from local to stage to prod):

### (1.) on-prem : MSSQL server(on-prem) to Bigquery(GCP) using Apache Spark.
It uses on-prem Apache Spark in docker image to run Pyspark script. Data is transferred from MS SQL Server to Bigquery using pyspark script. The transformation includes cleaning nulls, joining all three tables, column selection, feature addition.

### (2.) dev: Cloud SQL to Bigquery using Apache Spark
In this data is transferred from Google Cloud SQL SQL Server instance to Bigquery table using same script as on-prem. Similar transformations are used. The dev branch also introduces using Cloud SQL completely from GCP that is using Dataproc as in real world though full fledged testing will be done in stg and prod env.