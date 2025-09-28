import pandas as pd
from pyiceberg.catalog import load_catalog
from utils.config import (
    O3_ENDPOINT,
    O3_BUCKET,
    O3_REGION,
    O3_ACCESS_KEY,
    O3_SECRET_KEY,
)


def ingest_data(catalog, table_name):
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    table = catalog.load_table(table_name)
    # pyiceberg currently supports append via Arrow Table
    import pyarrow as pa
    arrow_table = pa.Table.from_pandas(df)
    table.append(arrow_table)
    print("Data ingested.")


if __name__ == "__main__":
    catalog = load_catalog(
        "s3",
        s3_endpoint=O3_ENDPOINT,
        s3_access_key_id=O3_ACCESS_KEY,
        s3_secret_access_key=O3_SECRET_KEY,
        s3_region=O3_REGION,
        warehouse=f"s3://{O3_BUCKET}/warehouse"
    )
    ingest_data(catalog, "iceberg_demo")
