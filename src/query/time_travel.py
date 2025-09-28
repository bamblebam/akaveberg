from pyiceberg.catalog import load_catalog
from utils.config import (
    O3_ENDPOINT,
    O3_BUCKET,
    O3_REGION,
    O3_ACCESS_KEY,
    O3_SECRET_KEY,
)


def time_travel_query(catalog, table_name, snapshot_id):
    table = catalog.load_table(table_name)
    df = table.scan(snapshot_id=snapshot_id).to_pandas()
    print(df)


def query_previous_snapshot(endpoint, bucket, table_name, snapshot_id):
    catalog = load_catalog(
        "s3",
        s3_endpoint=endpoint,
        s3_access_key_id=O3_ACCESS_KEY,
        s3_secret_access_key=O3_SECRET_KEY,
        s3_region=O3_REGION,
        warehouse=f"s3://{bucket}/warehouse"
    )
    table = catalog.load_table(table_name)
    df = table.scan(snapshot_id=snapshot_id).to_pandas()
    return df


if __name__ == "__main__":
    catalog = load_catalog(
        "s3",
        s3_endpoint=O3_ENDPOINT,
        s3_access_key_id=O3_ACCESS_KEY,
        s3_secret_access_key=O3_SECRET_KEY,
        s3_region=O3_REGION,
        warehouse=f"s3://{O3_BUCKET}/warehouse"
    )
    # Replace with your previous snapshot_id
    time_travel_query(catalog, "iceberg_demo", snapshot_id="1234567890")
