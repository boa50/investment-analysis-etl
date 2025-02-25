import pandas as pd
from data.db_creation import batch_load
from pathlib import Path


def load_dividends_into_db():
    try:
        file_path = "data/processed/stocks-prices.csv"
        df = pd.read_csv(file_path)

        batch_load.load_data(table_name="stocks-prices", df=df)

        print()
        print("Loaded new prices")

        Path(file_path).unlink(missing_ok=True)
    except FileNotFoundError:
        print("Prices file doesn't exist")
