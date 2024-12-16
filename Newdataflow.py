import apache_beam as beam
import csv
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, WorkerOptions, GoogleCloudOptions
import subprocess
import sys

# Configuration for Cloud SQL and PostgreSQL
DB_CONFIG = {
    "username": "postgres",
    "password": "Postgres@123",
    "database": "postgres",
    "query": "SELECT emp_id, first_name, last_name, email, hire_date, monthly_salary, department FROM employee",
    "host": "34.57.198.207",
    "port": "5432"
}

# Configuration for Google Pub/Sub
PUBSUB_CONFIG = {
    "topic": "projects/civic-circuit-440806-t2/topics/employee-data-topic",
    "subscription": "projects/civic-circuit-440806-t2/subscriptions/employee-data-sub"
}

# Function to calculate annual salary
def calculate_annual_salary(row):
    row['annual_salary'] = row['monthly_salary'] * 12
    return row

# Function to check data size
def is_data_large(rows):
    size_in_bytes = sum(len(str(row)) for row in rows)
    size_in_mb = size_in_bytes / (1024 * 1024)
    return size_in_mb > 10

# Function to write to PostgreSQL
def write_to_postgres_batch(rows, db_config):
    import psycopg2
    connection = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["username"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    cursor = connection.cursor()
    for row in rows:
        cursor.execute(
            """
            INSERT INTO employee_target (emp_id, first_name, last_name, email, hire_date, monthly_salary, department, annual_salary)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (row['emp_id'], row['first_name'], row['last_name'], row['email'], row['hire_date'], row['monthly_salary'], row['department'], row['annual_salary'])
        )
    connection.commit()
    cursor.close()
    connection.close()

# Function to publish data to Pub/Sub
class PublishToPubSub(beam.DoFn):
    def __init__(self, topic):
        self.topic = topic

    def setup(self):
        try:
            from google.cloud import pubsub_v1
            self.publisher = pubsub_v1.PublisherClient()
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "google-cloud-pubsub"])

    def process(self, row):
        import json
        topic_path = self.publisher.topic_path("civic-circuit-440806-t2", self.topic)
        message = json.dumps(row).encode("utf-8")
        self.publisher.publish(topic_path, data=message)
        yield row

# Function to subscribe to Pub/Sub and update the database
class ProcessFromPubSub(beam.DoFn):
    def __init__(self, subscription, db_config):
        self.subscription = subscription
        self.db_config = db_config

    def setup(self):
        try:
            from google.cloud import pubsub_v1
            self.subscriber = pubsub_v1.SubscriberClient()
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "google-cloud-pubsub"])

    def process(self, element):
        import json
        from concurrent.futures import TimeoutError

        subscription_path = self.subscriber.subscription_path("civic-circuit-440806-t2", self.subscription)
        response = self.subscriber.pull(subscription_path, max_messages=10)
        rows = []

        for message in response.received_messages:
            rows.append(json.loads(message.message.data))
            self.subscriber.acknowledge(subscription_path, [message.ack_id])

        write_to_postgres_batch(rows, self.db_config)
        yield "Processed and updated database from Pub/Sub messages"

# Function to read from PostgreSQL
class ReadFromPostgres(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config

    def setup(self):
        try:
            import psycopg2
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])

    def process(self, element):
        import psycopg2
        connection = psycopg2.connect(
            dbname=self.db_config["database"],
            user=self.db_config["username"],
            password=self.db_config["password"],
            host=self.db_config["host"],
            port=self.db_config["port"]
        )
        cursor = connection.cursor()
        cursor.execute(self.db_config["query"])
        rows = cursor.fetchall()
        for row in rows:
            yield {
                'emp_id': row[0],
                'first_name': row[1],
                'last_name': row[2],
                'email': row[3],
                'hire_date': row[4],
                'monthly_salary': row[5],
                'department': row[6],
            }
        cursor.close()
        connection.close()

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
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = "n1-standard-1"
    worker_options.autoscaling_algorithm = "THROUGHPUT_BASED"

    with beam.Pipeline(options=options) as pipeline:
        rows = (
            pipeline
            | "Start Pipeline" >> beam.Create([None])
            | "Read from PostgreSQL" >> beam.ParDo(ReadFromPostgres(DB_CONFIG))
        )

        transformed_rows = (
            rows
            | "Filter Valid Rows" >> beam.Filter(lambda row: 'monthly_salary' in row and row['monthly_salary'] is not None)
            | "Calculate Annual Salary" >> beam.Map(calculate_annual_salary)
        )

        large_data = (
            transformed_rows
            | "Filter Large Data" >> beam.Filter(lambda row: is_data_large([row]))
            | "Write to GCS CSV" >> beam.io.WriteToText(gcs_file_path, file_name_suffix='.csv', shard_name_template='')
        )

        small_data = (
            transformed_rows
            | "Filter Small Data" >> beam.Filter(lambda row: not is_data_large([row]))
            | "Publish to Pub/Sub" >> beam.ParDo(PublishToPubSub(PUBSUB_CONFIG["topic"]))
            | "Subscribe and Update DB" >> beam.ParDo(ProcessFromPubSub(PUBSUB_CONFIG["subscription"], DB_CONFIG))
        )

if __name__ == "__main__":
    run()
