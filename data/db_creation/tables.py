from google.cloud import bigquery
from schemas import get_schema
import os
import data.db as db

db.get_db_credentials()

# Construct a BigQuery client object.
client = bigquery.Client()

# Set the dataset_id
dataset_id = f"{os.environ.get('DB_PROJECT_ID')}.{os.environ.get('DB_DATASET_ID')}"


def create_table(table_name):
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = dataset_id + "." + table_name

    schema = get_schema(table_name=table_name)

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


# create_table("stocks-basic-info")
# create_table("stocks-fundaments")
# create_table("stocks-history")
# create_table("stocks-right-prices")
# create_table("stocks-ratings")
# create_table("ipca")
# create_table("stocks-dividends")
# create_table("stocks-dividends-docs-processed")
# create_table("stocks-dividends-old")
# create_table("stocks-splits")
