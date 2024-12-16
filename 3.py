import psycopg2

def is_table_large(db_config, size_threshold_mb=10):
    """Check if the table size is greater than the threshold (10MB by default)."""
    try:
        connection = psycopg2.connect(
            dbname=db_config["database"],
            user=db_config["username"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cursor = connection.cursor()
        cursor.execute("""
            SELECT pg_total_relation_size('employee') AS table_size;
        """)
        table_size_bytes = cursor.fetchone()[0]  # Get size in bytes
        table_size_mb = table_size_bytes / (1024 * 1024)  # Convert to MB
        cursor.close()
        connection.close()
        return table_size_mb >= size_threshold_mb
    except Exception as e:
        print(f"Error calculating table size: {e}")
        return True  # Default to "large" if there's an error
