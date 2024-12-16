def subscriber_pipeline():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "civic-circuit-440806-t2"
    google_cloud_options.temp_location = "gs://ramesh_bucket_for_a_poc/temp"
    google_cloud_options.region = "us-central1"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic="projects/civic-circuit-440806-t2/topics/employee-data-topic")
            | "Parse JSON" >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))
            | "Write to PostgreSQL" >> beam.Map(write_to_postgres_batch, DB_CONFIG)
        )

if __name__ == "__main__":
    subscriber_pipeline()
