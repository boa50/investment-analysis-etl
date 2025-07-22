# Based on https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table
from google.cloud import bigquery
from data.db_creation.schemas import get_schema
import numpy as np
import pandas as pd
from pathlib import Path
import os
import data.db as db

db.get_db_credentials()

# Construct a BigQuery client object.
client = bigquery.Client()

dataset_id = f"{os.environ.get('DB_PROJECT_ID')}.{os.environ.get('DB_DATASET_ID')}"


def load_data(table_name: str, df: pd.DataFrame):
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=get_schema(table_name=table_name),
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    tmp_file_path = "data/db_creation/tmp.csv"
    df.to_csv(tmp_file_path, index=False)

    with open(tmp_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

    Path(tmp_file_path).unlink(missing_ok=True)


def load_json_data(table_name: str, data: list):
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=get_schema(table_name=table_name),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(data, table_id, job_config=job_config)

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


### BASIC INFO
# df = pd.read_csv("data/processed/stocks-basic-info.csv")
# df = df[
#     [
#         "CD_CVM",
#         "NAME",
#         "TICKERS",
#         "NUM_COMMON",
#         "NUM_PREFERENTIAL",
#         "NUM_TOTAL",
#         "AVAILABLE_COMMON",
#         "AVAILABLE_PREFERENTIAL",
#         "AVAILABLE_TOTAL",
#         "FOUNDATION",
#         "GOVERNANCE_LEVEL",
#         "SECTOR",
#         "SUBSECTOR",
#         "SEGMENT",
#         "WEB_PAGE",
#     ]
# ]

# load_data(table_name="stocks-basic-info", df=df)


### FUNDAMENTS
# df = pd.read_csv("data/processed/stocks-fundaments.csv")

# df = df.replace([np.inf, -np.inf], np.nan)
# df.columns = ["CD_CVM", "DT_START", "DT_END", "KPI", "VALUE", "DT_YEAR", "VALUE_ROLLING_YEAR"]
# df = df[["CD_CVM", "DT_START", "DT_END", "DT_YEAR", "KPI", "VALUE", "VALUE_ROLLING_YEAR"]]

# table_name = "stocks-fundaments"

# load_data(table_name=table_name, df=df)


### HISTORY
# df = pd.read_csv("data/processed/stocks-history.csv")
# df.columns = [
#     "DT_EVENT",
#     "CD_CVM",
#     "TICKER",
#     "PRICE",
#     "PRICE_PROFIT",
#     "DIVIDEND_YIELD",
#     "DIVIDEND_PAYOUT",
#     "PRICE_EQUITY",
# ]
# table_name = "stocks-history"

# load_data(table_name=table_name, df=df)

### RIGHT PRICES
# df = pd.read_csv("data/processed/stocks-right-prices.csv")
# table_name = "stocks-right-prices"

# load_data(table_name=table_name, df=df)

### IPCA
# df = pd.read_csv("data/processed/ipca.csv", parse_dates=["DATE"])
# table_name = "ipca"

# load_data(table_name=table_name, df=df)

### STOCKS SPLITS
# df = pd.read_csv("data/processed/stocks-splits.csv", parse_dates=["DATE"])

# load_data(table_name="stocks-splits", df=df)
