from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from datetime import datetime, timedelta

# --- DAG Configuration ---
# Your Google Cloud project ID
PROJECT_ID = "zomato-stg"  # CHANGE THIS!
# The GCP region where Dataflow jobs will run
REGION = "us-central1"            # CHANGE THIS!
# GCS path for Dataflow's temporary and staging files
TEMP_LOCATION = "gs://df-avro-pq-bk/temp"  # CHANGE THIS!
# GCS path where your 'avro_to_parquet.py' script and 'requirements.txt' are uploaded
GCS_PYTHON_SCRIPT_LOCATION = "gs://df-avro-pq-bk/df_avro_parquet.py" # CHANGE THIS!
# GCS path to your requirements.txt if your Python script has custom dependencies
# GCS_REQUIREMENTS_FILE = "gs://df-avro-pq-gcs/requirements.txt" # CHANGE THIS! (or set to None if not needed)

# Airflow Variable Key for your table schemas JSON
TABLES_CONFIG_VARIABLE_KEY = "tables_config_json" # Ensure this matches your Airflow Variable name

with DAG(
    dag_id='avro_to_parquet_dataflow_daily',
    start_date=datetime(2025, 8, 11),
    # Set your desired schedule interval (e.g., daily, hourly, or a specific cron)
    # '@daily' means it runs once a day at the beginning of the day (midnight UTC)
    # For 2 AM UTC, use '0 2 * * *'
    schedule_interval='@daily',
    catchup=False, # Set to True if you want to run for past missed schedules
    tags=['data_pipeline', 'etl', 'dataflow', 'parquet'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    # --- Step 1: Fetch tables configuration from Airflow Variable ---
    # This step is implicit as Variable.get is called directly in the operator options.
    # We call it here just to demonstrate its retrieval.
    try:
        tables_config_json = Variable.get(TABLES_CONFIG_VARIABLE_KEY, deserialize_json=False)
        # You can add a log here if needed for debugging in Airflow logs
        # print(f"Fetched tables_config_json from Airflow Variable: {tables_config_json[:100]}...")
    except KeyError:
        raise ValueError(f"Airflow Variable '{TABLES_CONFIG_VARIABLE_KEY}' not found. Please create it.")
    except Exception as e:
        raise ValueError(f"Error fetching Airflow Variable '{TABLES_CONFIG_VARIABLE_KEY}': {e}")


    # --- Step 2: Start the Dataflow Job ---
    start_avro_to_parquet_job = DataflowStartPythonJobOperator(
        task_id="run_avro_to_parquet_dataflow_job",
        py_file=GCS_PYTHON_SCRIPT_LOCATION, # Path to your Beam Python script in GCS
        project_id=PROJECT_ID,
        location=REGION, # Dataflow job region
        # Dataflow job name (use Airflow macros for dynamic naming, e.g., ds_nodash for date)
        job_name=f"avro-to-parquet-{dag.dag_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        options={
            "temp_location": TEMP_LOCATION,
            # Pass the JSON string to your Python script via its argparse parameter
            "tables_config_json": tables_config_json,
            # Also explicitly pass project and region to the Python script as it expects them
            "project": PROJECT_ID,
            "region": REGION,
        },
        # If your Python script requires custom libraries defined in requirements.txt
        # make sure this file is also in GCS and accessible.
        requirements_file=GCS_REQUIREMENTS_FILE if GCS_REQUIREMENTS_FILE else None,
        # Default options for the Dataflow job runner (e.g., staging location)
        dataflow_default_options={
            "runner": "DataflowRunner",
            "staging_location": f"{TEMP_LOCATION}/staging"
        }
    )

