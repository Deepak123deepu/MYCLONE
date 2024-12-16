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

    # Check table size before starting the pipeline
    is_large_table = is_table_large(DB_CONFIG)

    with beam.Pipeline(options=options) as pipeline:
        rows = (
            pipeline
            | "Start Pipeline" >> beam.Create([None])  # Dummy PCollection
            | "Read from PostgreSQL" >> beam.ParDo(ReadFromPostgres(DB_CONFIG))
        )

        transformed_rows = (
            rows
            | "Filter Valid Rows" >> beam.Filter(lambda row: 'monthly_salary' in row and row['monthly_salary'] is not None)
            | "Calculate Annual Salary" >> beam.Map(calculate_annual_salary)
        )

        if is_large_table:
            # Branch: Large Table (≥ 10MB) -> GCS + PostgreSQL
            _ = (
                transformed_rows
                | "Write to GCS CSV" >> beam.io.WriteToText(gcs_file_path, file_name_suffix='.csv', shard_name_template='')
                | "Write to PostgreSQL" >> beam.Map(write_to_postgres_batch, DB_CONFIG)
            )
        else:
            # Branch: Small Table (< 10MB) -> Pub/Sub
            _ = (
                transformed_rows
                | "Publish to Pub/Sub" >> beam.ParDo(PublishToPubSub(PUBSUB_CONFIG["topic"]))
            )
