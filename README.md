# zomato-order-data

This project contains ETL pipeline to fetch data of zomato orders from OLTP to OLAP. It fetches data from 3 tables containg data of food, order item and order data.

![ER Diagram](/mssql_to_bq/images/Zomato_Physical_ER.png)
 
 
 # Zomato Order Data ETL Pipeline

This repository contains an end-to-end ETL pipeline for ingesting, transforming, and loading Zomato order data from a PostgreSQL OLTP source into Google BigQuery for analytics. The pipeline leverages Google Cloud Platform services, Apache Spark, and Apache Airflow for orchestration.

## Architecture Overview

1. **Data Ingestion (Datastream)**
   - Data is continuously replicated from PostgreSQL to Google Cloud Storage (GCS) as Avro files using [Google Datastream](https://cloud.google.com/datastream).

2. **Avro to Parquet Conversion (Airflow & Dataflow)**
   - Airflow DAGs (see [`mssql_to_bq/scripts/zomato-composer/`](mssql_to_bq/scripts/zomato-composer/)) orchestrate the conversion of Avro files in GCS to Parquet format using Google Dataflow.
   - Schemas for Avro files are managed in [`mssql_to_bq/scripts/df-avro-pq/`](mssql_to_bq/scripts/df-avro-pq/).

3. **Transformation & Load (Dataproc to BigQuery)**
   - A PySpark job ([`zomato-spark-gcs-bq.py`](mssql_to_bq/scripts/zomato-spark-gcs-bq.py)) reads the Parquet files from GCS, performs data cleaning, joins, and feature engineering.
   - The transformed data is loaded into BigQuery for analytics.

4. **Orchestration & Deployment**
   - Airflow DAGs in [zomato-composer](mssql_to_bq/scripts/zomato-composer/) automate the workflow.
   - GitHub Actions ([`.github/workflows/deploy-to-gcs.yml`](.github/workflows/deploy-to-gcs.yml)) automate deployment of scripts and DAGs to GCS and Composer environments.

## Pipeline Flow
![ER Diagram](/mssql_to_bq/images/ETLFlow.png)

1. **Postgres Cloud SQL → Avro (Datastream):**
   - Datastream replicates tables from Postgres to GCS as Avro files.

2. **Avro → Parquet (Airflow + Dataflow):**
   - Airflow DAG triggers Dataflow jobs to convert Avro files to Parquet using provided schemas.

3. **Parquet → BigQuery (Spark + Dataproc):**
   - PySpark job reads Parquet, transforms data, and loads it into BigQuery.

4. **Automation (GitHub Actions):**
   - On code changes, scripts and DAGs are automatically deployed to GCS and Composer.

## How to Run

- **Local Development:**  
  Use `docker-compose up` in the `mssql_to_bq` directory to start a local Spark environment.
- **Production:**  
  DAGs are deployed to Cloud Composer and jobs run on Dataproc/Dataflow as orchestrated by Airflow.

## Key Files

- [`zomato-spark-gcs-bq.py`](mssql_to_bq/scripts/zomato-spark-gcs-bq.py): Main Spark ETL script.
- [`zomato-gcs-bq-dag.py`](mssql_to_bq/scripts/zomato-composer/zomato-gcs-bq-dag.py): Airflow DAG for Avro to Parquet conversion.
- [`deploy-to-gcs.yml`](.github/workflows/deploy-to-gcs.yml): CI/CD workflow for deployment.

## Security

- Service account keys and sensitive configs are excluded via `.gitignore`.
- Secrets are managed via Google Secret Manager and Airflow Variables.

## License

MIT License. See [LICENSE](LICENSE) for details.
