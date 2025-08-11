from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from datetime import datetime

with DAG(
    dag_id='daily_avro_to_parquet_converter_direct', # New DAG ID
    start_date=datetime(2025, 8, 11),
    schedule_interval='@daily',
    catchup=False,
    tags=['data_pipeline', 'etl', 'daily', 'direct_dataflow'],
) as dag:
    # Retrieve the JSON string from Airflow Variable
    tables_config_json_airflow = Variable.get("multi_table_schemas_json", deserialize_json=False)

    # Define your common constants here, or as other Airflow Variables
    PROJECT_ID = "your-gcp-project-id"
    REGION = "us-central1"
    TEMP_LOCATION = "gs://your-dataflow-temp-bucket/temp"
    GCS_PYTHON_FILE_LOCATION = "gs://your-gcs-bucket-for-scripts/avro_to_parquet.py"
    # If you have a requirements.txt file for custom libraries:
    GCS_REQUIREMENTS_FILE = "gs://your-gcs-bucket-for-scripts/requirements.txt" # Make sure this exists if used

    start_dataflow_job = DataflowStartPythonJobOperator(
        task_id="start_avro_to_parquet_job",
        py_file=GCS_PYTHON_FILE_LOCATION,
        project_id=PROJECT_ID,
        location=REGION, # Use 'location' for region/zone in this operator
        job_name="multi-table-avro-to-parquet-{{ ds_nodash }}", # Dynamic job name
        options={
            "temp_location": TEMP_LOCATION,
            # Pass the JSON string as an argument to your Python script
            "tables_config_json": tables_config_json_airflow,
            "project": PROJECT_ID, # Passed to your Python script's argparse
            "region": REGION # Passed to your Python script's argparse
        },
        # Specify requirements file if you have custom libraries
        requirements_file=GCS_REQUIREMENTS_FILE, # Optional: if you have a requirements.txt
        dataflow_default_options={
            "runner": "DataflowRunner",
            "staging_location": f"{TEMP_LOCATION}/staging"
        }
    )