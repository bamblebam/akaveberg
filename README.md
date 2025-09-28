# Iceberg O3 MVP

This project implements a minimum viable product (MVP) for managing an Apache Iceberg table backed by O3. It includes functionalities for creating a table, ingesting data, and performing time travel queries on previous snapshots.

## Project Structure

```
iceberg-o3-mvp
├── src
│   ├── main.py
│   ├── table
│   │   └── create_table.py
│   ├── ingest
│   │   └── ingest_data.py
│   ├── query
│   │   └── time_travel.py
│   └── utils
│       └── config.py
├── requirements.txt
└── README.md
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd iceberg-o3-mvp
   ```

2. **Install dependencies:**
   Make sure you have Python 3.7 or higher installed. Then, run:
   ```
   pip install -r requirements.txt
   ```

3. **Configure O3 Endpoint:**
   Update the `src/utils/config.py` file with your O3 endpoint URL and bucket name.

## Usage

To run the application, execute the following command:
```
python src/main.py
```

This will create an Iceberg table, ingest a small batch of data, and demonstrate time travel querying.

## Core Features

- **Table Creation:** Initializes an Apache Iceberg table backed by O3.
- **Data Ingestion:** Appends a small batch of rows/files to the Iceberg table.
- **Time Travel Queries:** Allows querying of previous snapshots based on snapshot ID or timestamp.

## Stretch Goals

- Implement additional data formats for ingestion.
- Enhance error handling and logging.
- Add support for more complex queries on the Iceberg table.

## Starter Script for S3 Bucket on O3

To create an S3 bucket on O3, you can use the following starter script (to be implemented in a separate script file):

```python
import boto3

def create_bucket(bucket_name):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    print(f'Bucket {bucket_name} created successfully.')

# Call the function with your desired bucket name
create_bucket('your-bucket-name')
```

## License

This project is licensed under the MIT License. See the LICENSE file for more details.