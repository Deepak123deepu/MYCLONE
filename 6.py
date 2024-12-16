def calculate_query_size(rows):
    """Calculate the size of query results."""
    import json
    total_size_bytes = sum(len(json.dumps(row).encode("utf-8")) for row in rows)
    total_size_mb = total_size_bytes / (1024 * 1024)
    return total_size_mb

def fetch_rows_and_decide_output():
    """Fetch rows from the database and calculate their size."""
    import psycopg2

    connection = psycopg2.connect(
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
    )
    cursor = connection.cursor()
    cursor.execute(DB_CONFIG["query"])
    rows = cursor.fetchall()

    # Transform rows into dictionaries (matching the row structure in your code)
    rows_dict = [
        {
            "emp_id": row[0],
            "first_name": row[1],
            "last_name": row[2],
            "email": row[3],
            "hire_date": row[4],
            "monthly_salary": row[5],
            "department": row[6],
        }
        for row in rows
    ]

    # Calculate size of fetched rows
    total_size_mb = calculate_query_size(rows_dict)

    connection.close()
    cursor.close()

    return rows_dict, total_size_mb

def run_pipeline(rows, output_to_gcs):
    """Run the Apache Beam pipeline."""
    gcs_bucket = "ramesh_bucket_for_a_poc"
    gcs_directory = "Data_files"
    gcs_file_name = "transformed_employee_data.csv"
    gcs_file_path = f"gs://{gcs_bucket}/{gcs_directory}/{gcs_file_name}"

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Create PCollection from the fetched rows
        rows_pcollection = pipeline | "Create Rows" >> beam.Create(rows)

        if output_to_gcs:
            # Write to GCS
            _ = (
                rows_pcollection
                | "Transform for GCS" >> beam.Map(calculate_annual_salary)
                | "Write to GCS" >> beam.io.WriteToText(gcs_file_path, file_name_suffix=".csv", shard_name_template="")
            )

            # Optionally write to PostgreSQL
            _ = (
                rows_pcollection
                | "Write to PostgreSQL" >> beam.Map(write_to_postgres_batch, DB_CONFIG)
            )
        else:
            # Publish to Pub/Sub
            _ = (
                rows_pcollection
                | "Transform for Pub/Sub" >> beam.Map(calculate_annual_salary)
                | "Publish to Pub/Sub" >> beam.ParDo(PublishToPubSub(PUBSUB_CONFIG["topic"]))
            )

def main():
    """Main function to fetch rows, calculate size, and run the pipeline."""
    rows, total_size_mb = fetch_rows_and_decide_output()

    print(f"Total size of query results: {total_size_mb:.2f} MB")

    # Decide output path based on size
    output_to_gcs = total_size_mb >= 10

    # Run pipeline with the decision
    run_pipeline(rows, output_to_gcs)

if __name__ == "__main__":
    main() 
