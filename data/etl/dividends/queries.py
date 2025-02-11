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
