import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2

GCS_BUCKET = "your-gcs-bucket"
INDICATOR_FILE = f"gs://{GCS_BUCKET}/pipeline1_output/indicator.json"

PUBSUB_TOPIC = "projects/your-project-id/topics/employee-data"
GCS_FILE_PATH = f"gs://{GCS_BUCKET}/pipeline1_output/employee_data.json"

def write_indicator_file(data_source):
    """Write an indicator file to GCS."""
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob("pipeline1_output/indicator.json")
    blob.upload_from_string(json.dumps({"source": data_source}))

def fetch_data_from_db():
    # (Fetch data from DB - same as before)

def calculate_size(rows):
    # (Calculate size of rows in MB - same as before)

class PublishToPubSub(beam.DoFn):
    # (Same as before)

def run_pipeline_1():
    rows = fetch_data_from_db()
    total_size_mb = calculate_size(rows)

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        rows_pcollection = pipeline | "Create Rows" >> beam.Create(rows)

        if total_size_mb >= 10:
            _ = (
                rows_pcollection
                | "Write to GCS" >> beam.io.WriteToText(GCS_FILE_PATH, shard_name_template="", file_name_suffix=".json")
            )
            write_indicator_file("gcs")  # Add this line
        else:
            _ = rows_pcollection | "Publish to Pub/Sub" >> beam.ParDo(PublishToPubSub(PUBSUB_TOPIC))
            write_indicator_file("pubsub")  # Add this line

if __name__ == "__main__":
    run_pipeline_1()






class ReadIndicatorFile(beam.DoFn):
    """Read the indicator file from GCS."""
    def process(self, element):
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket("your-gcs-bucket")
        blob = bucket.blob("pipeline1_output/indicator.json")
        indicator_content = json.loads(blob.download_as_text())
        yield indicator_content["source"]

class TransformData(beam.DoFn):
    # (Same as before)

class WriteToPostgres(beam.DoFn):
    # (Same as before)

def run_pipeline_2():
    gcs_path = f"gs://{GCS_BUCKET}/pipeline1_output/employee_data.json"

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        source_type = (
            pipeline
            | "Start Pipeline" >> beam.Create([None])  # Dummy PCollection
            | "Read Indicator File" >> beam.ParDo(ReadIndicatorFile())
        )

        data = (
            source_type
            | "Decide Source" >> beam.FlatMap(
                lambda source: beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
                if source == "pubsub"
                else beam.io.ReadFromText(gcs_path)
            )
            | "Parse JSON" >> beam.Map(json.loads)
        )

        transformed_data = data | "Transform Data" >> beam.ParDo(TransformData())

        _ = transformed_data | "Write to PostgreSQL" >> beam.ParDo(WriteToPostgres())

        archive_path = f"gs://{GCS_BUCKET}/pipeline2_output/transformed_employee_data.json"
        _ = (
            transformed_data
            | "Write Archive to GCS" >> beam.io.WriteToText(archive_path, shard_name_template="", file_name_suffix=".json")
        )

if __name__ == "__main__":
    run_pipeline_2()
