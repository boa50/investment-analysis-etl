from google.cloud import bigquery
import data.db as db

db.get_db_credentials()


def delete_old_data():
    sql = f"DELETE FROM {db.get_table_full_name('stocks-ratings')} WHERE true"

    client = bigquery.Client()

    query = client.query(sql)

    query.result()

    print("Deleted old stocks ratings")
