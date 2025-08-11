import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging

# Configure logging for better visibility in Dataflow logs
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)

# --- Helper Function to Create PyArrow Schema from Dictionary ---
def create_pyarrow_schema(schema_dict):
    """
    Converts a dictionary representation of a schema into a PyArrow schema.
    This function handles common SQL-to-PyArrow type mappings.
    Adjust this function to include any specific complex types (e.g., arrays, structs)
    if your Datastream Avro output has them beyond simple primitives.
    """
    fields = []
    for field_name, field_type_str in schema_dict.items():
        field_type = None
        if field_type_str == 'int64':
            field_type = pa.int64()
        elif field_type_str == 'string':
            field_type = pa.string()
        elif field_type_str == 'float64':
            field_type = pa.float64()
        elif field_type_str == 'timestamp':
            # Datastream often uses microsecond precision for Avro timestamps
            field_type = pa.timestamp('us', tz='UTC') # Assuming UTC timezone for timestamps
        elif field_type_str == 'boolean':
            field_type = pa.bool_()
        elif field_type_str == 'date':
            field_type = pa.date32() # For date without time
        # Add more type mappings as needed, e.g., for bytes, decimals, etc.
        else:
            _LOGGER.warning(f"Unsupported PyArrow type string encountered: {field_type_str}. Defaulting to string.")
            field_type = pa.string() # Default to string for unknown types to prevent failure

        fields.append(pa.field(field_name, field_type))
    return pa.schema(fields)

# --- Main Pipeline Function ---
def run_pipeline(tables_config_json, temp_location, project_id, region, pipeline_args):
    """
    Runs the Apache Beam pipeline to convert Avro files to Parquet for multiple tables.

    Args:
        tables_config_json (str): JSON string containing configuration for each table
                                  (input_path, output_path, schema).
        temp_location (str): GCS path for Dataflow temporary and staging files.
        project_id (str): Google Cloud project ID.
        region (str): The GCP region where the Dataflow job will run.
        pipeline_args (list): Additional arguments for the Beam pipeline options.
    """
    # Parse PipelineOptions from the arguments
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    standard_options = options.view_as(StandardOptions)

    # Set required Dataflow options from arguments
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.temp_location = temp_location
    standard_options.runner = 'DataflowRunner' # Ensure it runs on Dataflow

    _LOGGER.info(f"Pipeline options: {options.get_all_options()}")

    # Parse the tables configuration JSON string
    try:
        tables_config = json.loads(tables_config_json)
        _LOGGER.info(f"Parsed tables configuration for {len(tables_config)} tables.")
    except json.JSONDecodeError as e:
        _LOGGER.error(f"Failed to parse tables_config_json: {e}")
        raise

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        for table_name, config in tables_config.items():
            input_avro_path = config['input_avro_path']
            output_parquet_path = config['output_parquet_path']
            
            # Create PyArrow schema for the current table
            try:
                table_parquet_schema = create_pyarrow_schema(config['schema'])
                _LOGGER.info(f"Successfully created schema for table: {table_name}")
            except ValueError as e:
                _LOGGER.error(f"Schema creation failed for table {table_name}: {e}")
                raise

            _LOGGER.info(f"Processing table: {table_name}")
            _LOGGER.info(f"  Input Avro Path: {input_avro_path}")
            _LOGGER.info(f"  Output Parquet Path: {output_parquet_path}")

            # Define the pipeline steps for each table
            (p
             | f'Read Avro for {table_name}' >> beam.io.ReadFromAvro(input_avro_path)
             # Example optional transformation: Filter out Datastream internal metadata if not needed in Parquet
             # This is a common pattern if you only want the original source columns.
             # You would need to ensure your parquet_schema doesn't include these filtered fields.
             # For now, the schema explicitly includes them. If you uncomment the line below,
             # remove metadata fields from your parquet_schema definition in the Airflow variable.
             # | f'Filter Datastream Metadata for {table_name}' >> beam.Map(
             #     lambda record: {k: v for k, v in record.items() if not k.startswith('_metadata_')})
             | f'Write Parquet for {table_name}' >> beam.io.WriteToParquet(
                 file_path_prefix=output_parquet_path,
                 schema=table_parquet_schema,
                 codec='snappy', # Snappy is generally a good balance of compression and performance
                 file_name_suffix='.parquet',
                 num_shards=0 # Let Dataflow determine the optimal number of shards
             ))
    _LOGGER.info("Dataflow pipeline finished for all tables.")


# --- Main Execution Block --
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Apache Beam pipeline to convert Avro to Parquet for multiple tables.")
    # Arguments expected from DataflowStartPythonJobOperator's 'options'
    parser.add_argument(
        '--tables_config_json',
        required=True,
        help='JSON string containing configuration for each table (input_path, output_path, schema).')
    parser.add_argument(
        '--temp_location',
        required=True,
        help='GCS path for Dataflow temporary files (e.g., gs://your-dataflow-temp-bucket/temp)')
    parser.add_argument(
        '--project',
        required=True,
        help='Your Google Cloud project ID.')
    parser.add_argument(
        '--region',
        required=True,
        help='The GCP region where the Dataflow job will run (e.g., us-central1).')

    # Parse known arguments and pass remaining (pipeline_args) to Beam
    known_args, pipeline_args = parser.parse_known_args()

    # Call the pipeline function with parsed arguments
    run_pipeline(
        tables_config_json=known_args.tables_config_json,
        temp_location=known_args.temp_location,
        project_id=known_args.project,
        region=known_args.region,
        pipeline_args=pipeline_args
    )
