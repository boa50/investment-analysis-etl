import data_download
import queries
import db_load

try:
    data_download.get_latest_prices()
    queries.delete_outdated_prices()
    db_load.load_dividends_into_db()
except FileNotFoundError:
    print("Prices file doesn't exist")
