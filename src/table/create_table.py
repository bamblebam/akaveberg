from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType
from utils.config import (
    O3_ENDPOINT,
    O3_BUCKET,
    O3_REGION,
    O3_ACCESS_KEY,
    O3_SECRET_KEY,
)


def create_table(catalog, table_name):
    schema = Schema(
        [
            ("id", IntegerType()),
            ("name", StringType()),
        ]
    )
    catalog.create_table(table_name, schema)
    print(f"Table {table_name} created.")


if __name__ == "__main__":
    catalog = load_catalog(
        "s3",
        s3_endpoint=O3_ENDPOINT,
        s3_access_key_id=O3_ACCESS_KEY,
        s3_secret_access_key=O3_SECRET_KEY,
        s3_region=O3_REGION,
        warehouse=f"s3://{O3_BUCKET}/warehouse"
    )
    create_table(catalog, "iceberg_demo")
