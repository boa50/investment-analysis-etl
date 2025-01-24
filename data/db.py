import pandas_gbq as pdgbq
from data.set_env import set_env
import google.auth
import os

set_env()

project_id = os.environ.get("DB_PROJECT_ID")
dataset_id = os.environ.get("DB_DATASET_ID")

credentials = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


def get_table_full_name(table_name):
    return "`{}.{}.{}`".format(project_id, dataset_id, table_name)


def execute_query(sql):
    return pdgbq.read_gbq(sql, project_id=project_id)
