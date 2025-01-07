# Based on https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table
from google.cloud import bigquery
from schemas import get_schema
import numpy as np
import pandas as pd
from pathlib import Path

# Construct a BigQuery client object.
client = bigquery.Client()

dataset_id = "lazy-investor-db.app_dataset"

def load_data(table_name, df):

# TODO(developer): Set table_id to the ID of the table to create.
    table_id = dataset_id + "." + table_name

    job_config = bigquery.LoadJobConfig(
        schema=get_schema(table_name=table_name),
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )
    
    tmp_file_path = "data/db-creation/tmp.csv"
    df.to_csv(tmp_file_path, index=False)

    with open(tmp_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

    Path(tmp_file_path).unlink(missing_ok=True)


df = pd.read_csv("data/processed/stocks-fundaments.csv")

df = df.replace([np.inf, -np.inf], np.nan)
df.columns = ["CD_CVM", "DT_START", "DT_END", "KPI", "VALUE", "DT_YEAR", "VALUE_ROLLING_YEAR"]
df = df[["CD_CVM", "DT_START", "DT_END", "DT_YEAR", "KPI", "VALUE", "VALUE_ROLLING_YEAR"]]

table_name = "stocks-fundaments"

load_data(table_name=table_name, df=df)