from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

# DAG args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

PROJECT_ID = "zomato-stg"
REGION = "asia-south1"
CLUSTER_NAME = "my-zomato-dataproc-cluster"

PYSPARK_JOB = {
    "reference": {"job_id": "sqlserver_to_bq_job"},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://zomato-bucket-stg/pyspark/sqlserver_to_bq_job.py",
        "jar_file_uris": [
            "gs://zomato-bucket-stg/pyspark/jars/mssql-jdbc-12.10.1.jre11.jar",
            "gs://zomato-bucket-stg/pyspark/jars/gcs-connector-hadoop3-latest.jar"
        ],
        "archives": [
            "gs://zomato-bucket-stg/pyspark/temp_dataproc_venv_archive.zip#temp_dataproc_venv"
        ],
        "py_files": [
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
            "dataproc.conscrypt.provider.enable": "false",
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
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "metadata": {"PIP_PACKAGES_PROP": "google-cloud-secret-manager==2.24.0"},
        "initialization_actions": ["gs://zomato-bucket-stg/init-actions/cloudsql-proxy-init.sh"]
    }
}

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

    create_cluster >> submit_job >> delete_cluster
