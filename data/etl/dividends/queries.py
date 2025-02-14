from google.cloud import bigquery
import pandas_gbq as pdgbq
import data.db as db
import os


db.get_db_credentials()

project_id = os.environ.get("DB_PROJECT_ID")
dataset_id = os.environ.get("DB_DATASET_ID")


def get_table_full_name(table_name):
    return "`{}.{}.{}`".format(project_id, dataset_id, table_name)


def execute_query(sql):
    return pdgbq.read_gbq(sql, project_id=project_id)


def get_available_tickers():
    sql = f"""
            SELECT distinct TICKER
            FROM {get_table_full_name("stocks-history")}
        """

    available_tickers = execute_query(sql)["TICKER"].values

    return available_tickers


def get_available_cds_cvm():
    sql = f"""
            SELECT CD_CVM
            FROM {get_table_full_name("stocks-basic-info")}
        """

    available_cds_cvm = execute_query(sql)["CD_CVM"].values

    return available_cds_cvm


def get_already_processed_files():
    sql = f"""
            SELECT FILE_NAME
            FROM {get_table_full_name("stocks-dividends-docs-processed")}
        """

    already_processed_files = execute_query(sql)["FILE_NAME"].values

    return already_processed_files


def get_stocks_splits():
    sql = f"""
            SELECT *
            FROM {get_table_full_name("stocks-splits")}
        """

    return execute_query(sql)


def get_cd_cvm_by_ticker(ticker):
    sql = f"""
            SELECT CD_CVM 
            FROM {get_table_full_name("stocks-basic-info")} 
            WHERE TICKERS LIKE '%{ticker.upper()}%'
        """

    cd_cvm = execute_query(sql).iloc[0]["CD_CVM"]

    return cd_cvm


def delete_outdated_dividends(ticker: str, doc_date: str, doc_version: int):
    sql = f"""
            DELETE
            FROM {get_table_full_name("stocks-dividends")} 
            WHERE TICKER = '{ticker.upper()}'
                AND DOC_DATE = '{doc_date}'
                AND VERSION < {doc_version}
        """

    client = bigquery.Client()

    query = client.query(sql)

    query.result()

    print("Deleted outdated dividends")


def get_all_custom_dividends():
    sql = f"""
            SELECT *
            FROM {get_table_full_name("stocks-dividends")}
            WHERE VERSION = -1
        """

    return execute_query(sql)
