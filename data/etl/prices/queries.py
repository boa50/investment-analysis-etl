from google.cloud import bigquery
import data.db as db
import os


db.get_db_credentials()

project_id = os.environ.get("DB_PROJECT_ID")
dataset_id = os.environ.get("DB_DATASET_ID")


def delete_outdated_prices():
    sql = f"""
            DELETE FROM {db.get_table_full_name("stocks-prices")} 
            WHERE DATE = (SELECT MAX(DATE) FROM {db.get_table_full_name("stocks-prices")})
        """

    client = bigquery.Client()

    query = client.query(sql)

    query.result()

    print("Deleted outdated prices")


def get_last_load_date():
    sql = f"""
            SELECT MAX(DATE) AS DATE FROM {db.get_table_full_name("stocks-prices")}
        """

    return db.execute_query(sql).iat[0, 0]


def get_available_tickers():
    sql = f"""
            SELECT TICKER FROM {db.get_table_full_name("stocks-available")}
        """

    return db.execute_query(sql)["TICKER"].values
