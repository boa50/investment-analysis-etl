import pandas_gbq as pdgbq
from data.set_env import set_env
import google.auth
import os


def get_db_credentials():
    set_env()

    credentials = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return credentials


def get_table_full_name(table_name):
    return "`{}.{}.{}`".format(
        os.environ.get("DB_PROJECT_ID"), os.environ.get("DB_DATASET_ID"), table_name
    )


def execute_query(sql):
    return pdgbq.read_gbq(sql, project_id=os.environ.get("DB_PROJECT_ID"))
