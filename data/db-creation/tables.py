from google.cloud import bigquery
from schemas import get_schema

# Construct a BigQuery client object.
client = bigquery.Client()

# Set the dataset_id
dataset_id = "lazy-investor-db.app_dataset"

def create_table(table_name, schema):
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = dataset_id + "." + table_name

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    
table_name = "stocks-fundaments"
schema = get_schema(table_name=table_name)
create_table(table_name=table_name, schema=schema)
