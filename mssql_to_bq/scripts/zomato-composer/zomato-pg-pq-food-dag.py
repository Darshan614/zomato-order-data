from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
from airflow.utils.dates import days_ago

def extract_to_parquet(**context):
    hook = PostgresHook(postgres_conn_id="cloudsql_postgres")
    df = hook.get_pandas_df("SELECT * FROM food.allfooditems")
    df.to_parquet("/tmp/output.parquet", engine="pyarrow", index=False)

with DAG(
    "postgres_to_parquet_gcs",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_to_parquet",
        python_callable=extract_to_parquet
    )

    upload = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/output.parquet",
        dst="allfooditems/",  # path inside the bucket
        bucket="zomato-parquet-dump"
    )

    trigger_zomato = TriggerDagRunOperator(
        task_id="trigger_zomato_avro_to_parquet",
        trigger_dag_id="zomato_avro_to_parquet",  # the other DAG's ID
        wait_for_completion=False  # set True if you want to wait
    )

    extract >> upload >> trigger_zomato
