from pyiceberg.catalog import load_catalog
from table.create_table import create_table
from ingest.ingest_data import ingest_data
from query.time_travel import query_previous_snapshot
from utils.config import (
    O3_ENDPOINT,
    O3_BUCKET,
    O3_REGION,
    O3_ACCESS_KEY,
    O3_SECRET_KEY,
)


def main():
    # Set up the Iceberg S3 catalog
    catalog = load_catalog(
        "s3",
        s3_endpoint=O3_ENDPOINT,
        s3_access_key_id=O3_ACCESS_KEY,
        s3_secret_access_key=O3_SECRET_KEY,
        s3_region=O3_REGION,
        uri=f"s3://{O3_BUCKET}/warehouse"
    )

    # Create the Iceberg table
    table_name = "my_iceberg_table"
    create_table(catalog, table_name)

    # Ingest data into the Iceberg table
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ]
    ingest_data(catalog, table_name, data)

    # Query a previous snapshot (for demonstration, we assume a snapshot ID or timestamp)
    snapshot_id = "latest"  # Replace with actual snapshot ID or timestamp as needed
    previous_data = query_previous_snapshot(catalog, table_name, snapshot_id)
    print("Previous Snapshot Data:", previous_data)


if __name__ == "__main__":
    main()
