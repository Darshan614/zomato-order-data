from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage

# DAG args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

PROJECT_ID = "zomato-stg"
REGION = "asia-south1"
CLUSTER_NAME = "my-zomato-dataproc-cluster-new"

PYSPARK_JOB = {
    "reference": {"job_id": "zomato_gcs_to_bq_job"},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://zomato-bucket-stg/pyspark/zomato-spark-gcs-bq.py",
        "jar_file_uris": [
            "gs://zomato-bucket-stg/pyspark/jars/mssql-jdbc-12.10.1.jre11.jar",
            "gs://zomato-bucket-stg/pyspark/jars/gcs-connector-hadoop3-latest.jar"
        ],
        "archive_uris": [
            "gs://zomato-bucket-stg/pyspark/temp_dataproc_venv_archive.zip#temp_dataproc_venv"
        ],
        "python_file_uris": [
            "gs://zomato-bucket-stg/pyspark/dependencies.zip"
        ],
        "properties": {
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./temp_dataproc_venv/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "./temp_dataproc_venv/bin/python",
            "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "./temp_dataproc_venv/bin/python",
            "spark.executorEnv.PYTHONPATH": "./temp_dataproc_venv/lib/python3.10/site-packages:/workspace/dependencies.zip",
            "spark.yarn.appMasterEnv.PYTHONPATH": "./temp_dataproc_venv/lib/python3.10/site-packages:/workspace/dependencies.zip",
            "spark.driver.extraClassPath": "mssql-jdbc-12.10.1.jre11.jar",
            "spark.executor.extraClassPath": "mssql-jdbc-12.10.1.jre11.jar"
        },
        "args": [
            "--env", "stg",
            "--gcp-project-id", PROJECT_ID,
            "--bq-dataset-name", "zomato_spark_stg_bq",
            "--bq-temp-gcs-bucket", "zomato-bucket-stg"
        ]
    }
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_size_gb": 50}
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_size_gb": 50}
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "dataproc:pip.packages": "google-cloud-secret-manager==2.24.0",
            "dataproc:conscrypt.provider.enable": "false", 
            "spark:spark.jars": ",".join([
                "gs://zomato-bucket-stg/pyspark/jars/mssql-jdbc-12.10.1.jre11.jar",
                "gs://zomato-bucket-stg/pyspark/jars/spark-3.5-bigquery-0.35.0.jar",
                "gs://zomato-bucket-stg/pyspark/jars/gcs-connector-hadoop3-latest.jar",
                "gs://zomato-bucket-stg/pyspark/jars/cloud-sql-connector-jdbc-sqlserver-1.25.1.jar"
            ])
        }
    },
    "gce_cluster_config": {
        "service_account": "cloudsql-proxy-sa@zomato-stg.iam.gserviceaccount.com",
        "metadata": {"PIP_PACKAGES_PROP": "google-cloud-secret-manager==2.24.0"}
    },
    "initialization_actions": [
        {"executable_file": "gs://zomato-bucket-stg/init-actions/cloudsql-proxy-init.sh"}
    ]
}

ARCHIVE_PREFIX = "archive"
def move_parquet_to_archive():
    client = storage.Client(project=PROJECT_ID)
    bucket_name = "zomato-parquet-dump"
    folders = ["allfooditems", "ord_ordered_items", "ord_zomato_orders"]

    bucket = client.bucket(bucket_name)
    for folder in folders:
        blobs = client.list_blobs(bucket_name, prefix=f"{folder}/")
        for blob in blobs:
            # Skip folders
            if blob.name.endswith("/"):
                continue
            # Destination path
            dest_name = f"{ARCHIVE_PREFIX}/{blob.name}"
            new_blob = bucket.rename_blob(blob, dest_name)
            print(f"Moved {blob.name} to {dest_name}")

with DAG(
    "dataproc_pyspark_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done"  # Ensures deletion even if job fails
    )

    archive_parquet = PythonOperator(
        task_id="archive_parquet_files",
        python_callable=move_parquet_to_archive
    )

    create_cluster >> submit_job >> delete_cluster >> archive_parquet
