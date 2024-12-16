import json
import apache_beam as beam
from apache_beam.pvalue import AsSingleton

def calculate_query_result_size(rows):
    """Calculate the size of the fetched data in bytes."""
    total_size_bytes = sum(len(json.dumps(row).encode('utf-8')) for row in rows)
    total_size_mb = total_size_bytes / (1024 * 1024)  # Convert to MB
    return total_size_mb

class DecideOutputPath(beam.DoFn):
    """Decide whether to send data to GCS or Pub/Sub based on size."""
    def __init__(self, size_threshold_mb):
        self.size_threshold_mb = size_threshold_mb

    def process(self, row, total_size):
        if total_size >= self.size_threshold_mb:
            yield beam.pvalue.TaggedOutput('large', row)
        else:
            yield beam.pvalue.TaggedOutput('small', row)

def run():
    gcs_bucket = "ramesh_bucket_for_a_poc"
    gcs_directory = "Data_files"
    gcs_file_name = "transformed_employee_data.csv"
    gcs_file_path = f"gs://{gcs_bucket}/{gcs_directory}/{gcs_file_name}"

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "civic-circuit-440806-t2"
    google_cloud_options.temp_location = f"gs://{gcs_bucket}/temp"
    google_cloud_options.region = "us-central1"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as pipeline:
        # Read data from PostgreSQL
        rows = (
            pipeline
            | "Start Pipeline" >> beam.Create([None])  # Dummy element to trigger the pipeline
            | "Read from PostgreSQL" >> beam.ParDo(ReadFromPostgres(DB_CONFIG))
        )

        # Collect rows and calculate the size of fetched data
        rows_as_list = rows | "Collect Rows" >> beam.combiners.ToList()
        query_result_size_mb = rows_as_list | "Calculate Query Result Size" >> beam.Map(calculate_query_result_size)

        # Split rows into 'large' and 'small' based on the size of the fetched data
        split_rows = (
            rows
            | "Decide Path Based on Size" >> beam.ParDo(
                DecideOutputPath(size_threshold_mb=10), total_size=AsSingleton(query_result_size_mb)
            ).with_outputs('large', 'small')
        )

        # Branch 1: Large data (>= 10 MB) -> Write to GCS and PostgreSQL
        large_data = split_rows.large
        _ = (
            large_data
            | "Transform Large Data" >> beam.Map(calculate_annual_salary)
            | "Write Large Data to GCS" >> beam.io.WriteToText(gcs_file_path, file_name_suffix='.csv', shard_name_template='')
        )

        _ = (
            large_data
            | "Write Large Data to PostgreSQL" >> beam.Map(write_to_postgres_batch, DB_CONFIG)
        )

        # Branch 2: Small data (< 10 MB) -> Publish to Pub/Sub
        small_data = split_rows.small
        _ = (
            small_data
            | "Publish Small Data to Pub/Sub" >> beam.ParDo(PublishToPubSub(PUBSUB_CONFIG["topic"]))
        ) 
