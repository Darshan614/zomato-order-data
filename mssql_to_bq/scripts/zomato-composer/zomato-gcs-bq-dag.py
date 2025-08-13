from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from google.cloud import storage
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

PROJECT_ID = "zomato-stg"
REGION = "asia-south2"

def get_previous_date_prefix():
    """Returns previous date in YYYY/MM/DD format for GCS folder filtering."""
    prev_date = datetime.utcnow() - timedelta(days=1)
    return prev_date.strftime("%Y/%m/%d")  # e.g., 2025/08/12

# GCS locations and schema paths for both tables
TABLES = {
    "ord_zomato_orders": {
        "gcs_pattern": "gs://zomato-oltp-avro-dump/dump/ord_zomato_orders/**/*.avro",
        "schema": "gs://df-avro-pq-bk/avro_schema_orders.avsc"
    },
    "ord_ordered_items": {
        "gcs_pattern": "gs://zomato-oltp-avro-dump/dump/ord_ordered_items/**/*.avro",
        "schema": "gs://df-avro-pq-bk/avro_schema_oitems.avsc"
    }
}

ARCHIVE_PATH = "gs://zomato-oltp-avro-dump/archive"

def check_gcs_files(gcs_pattern, **kwargs):
    bucket_name = gcs_pattern.split("/")[2]
    date_prefix = get_previous_date_prefix()
    prefix = "/".join(gcs_pattern.split("/")[3:]).replace("**/*.avro", f"{date_prefix}/")

    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))

    if not blobs:
        EmailOperator(
            task_id=f'email_no_files_{prefix.replace("/", "_")}',
            to="jaindarshan849@gmail.com",
            subject="No AVRO files found for processing",
            html_content=f"<p>No AVRO files found in path: {gcs_pattern}</p>"
        ).execute(context=kwargs)
        raise AirflowSkipException(f"No files found for {gcs_pattern}")

def move_files_to_archive(gcs_pattern, **kwargs):
    bucket_name = gcs_pattern.split("/")[2]
    date_prefix = get_previous_date_prefix()
    prefix = "/".join(gcs_pattern.split("/")[3:]).replace("**/*.avro", f"{date_prefix}/")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))

    for blob in blobs:
        # Get relative path inside the prefix folder (e.g. ord_zomato_orders/2025/08/11/file.avro)
        relative_path = blob.name[len(prefix):].lstrip("/")

        # Skip files directly under the prefix folder to retain the folder itself (like dump/ord_zomato_orders/file.avro)
        if "/" not in relative_path:
            continue

        table_folder = prefix.split("/")[-1]  # 'ord_zomato_orders'
        # Compose the path inside archive with table folder included
        dest_blob_name = f"archive/{table_folder}/{date_prefix}/{relative_path}"

        bucket.rename_blob(blob, dest_blob_name)


with DAG(
    dag_id="zomato_avro_to_parquet",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["zomato", "dataflow", "avro-parquet"],
) as dag:

    move_tasks = []

    for table_name, config in TABLES.items():
        gcs_pattern = config["gcs_pattern"]
        schema_path = config["schema"]

        precheck = PythonOperator(
            task_id=f"check_files_{table_name}",
            python_callable=check_gcs_files,
            op_kwargs={"gcs_pattern": gcs_pattern},
            provide_context=True
        )

        dataflow_task = DataflowStartFlexTemplateOperator(
            task_id=f"avro_to_parquet_{table_name}",
            body={
                "launchParameter": {
                    "jobName": f"{table_name.replace('_', '-')}-avro-pq-del",
                    "containerSpecGcsPath": "gs://dataflow-templates-asia-south2/latest/flex/File_Format_Conversion",
                    "parameters": {
                        "inputFileFormat": "avro",
                        "outputFileFormat": "parquet",
                        "inputFileSpec": gcs_pattern.replace("**/*.avro", f"{get_previous_date_prefix()}/*.avro"),
                        "containsHeaders": "false",
                        "csvFormat": "Default",
                        "largeNumFiles": "false",
                        "csvFileEncoding": "UTF-8",
                        "logDetailedCsvConversionErrors": "false",
                        "outputBucket": f"gs://zomato-parquet-dump/{table_name}",
                        "schema": schema_path,
                        "numShards": "0",
                        "outputFilePrefix": "output",
                    },
                }
            },
            location=REGION,
            project_id=PROJECT_ID,
        )

        move_to_archive = PythonOperator(
            task_id=f"move_to_archive_{table_name}",
            python_callable=move_files_to_archive,
            op_kwargs={"gcs_pattern": gcs_pattern},
            provide_context=True
        )

        precheck >> dataflow_task >> move_to_archive
        move_tasks.append(move_to_archive)

    # final_email = EmailOperator(
    #     task_id="send_completion_email",
    #     to="jaindarshan849@gmail.com",
    #     subject="Zomato Avro to Parquet DAG Completed",
    #     html_content="<p>The Zomato Avro to Parquet dataflow processing DAG has completed successfully.</p>",
    # )

    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_dag",
        trigger_dag_id="dataproc_pyspark_pipeline",
        wait_for_completion=False
    )

    move_tasks >> trigger_spark
