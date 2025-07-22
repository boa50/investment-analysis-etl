import pandas as pd
import decimal
from data.db_creation import batch_load
import data.etl.queries.queries as qu
from pathlib import Path


def _calculate_bazin(constant=0.06, n_years=3):
    df_dividends = qu.get_dividends()

    df_dividends["DATE"] = pd.to_datetime(df_dividends["DATE"])

    df_dividends_grorup = df_dividends.drop("VALUE", axis=1)
    df_dividends_grorup = df_dividends_grorup.groupby("TICKER").max().reset_index()

    df_dividends_grorup["DATE_OFFSET"] = df_dividends["DATE"] - pd.DateOffset(
        years=n_years
    )
    df_dividends_grorup = df_dividends_grorup.drop("DATE", axis=1)

    df_bazin = df_dividends.merge(df_dividends_grorup, on=["TICKER"])

    df_bazin = df_bazin[df_bazin["DATE"] > df_bazin["DATE_OFFSET"]]

    df_bazin = df_bazin.drop(["DATE", "DATE_OFFSET"], axis=1)

    df_bazin = df_bazin.groupby("TICKER").sum().reset_index()

    df_bazin["VALUE"] = df_bazin["VALUE"] / n_years / decimal.Decimal(str(constant))

    df_bazin["VALUE"] = df_bazin["VALUE"].astype(float)

    return df_bazin


def calculate_right_prices():
    df_prices = _calculate_bazin()
    df_prices = df_prices.rename(columns={"VALUE": "BAZIN"})

    df_prices.to_csv("data/processed/stocks-right-prices.csv", index=False)


def load_prices_into_db():
    try:
        file_path = "data/processed/stocks-right-prices.csv"
        df = pd.read_csv(file_path)

        batch_load.load_data(table_name="stocks-right-prices", df=df)

        print()
        print("Loaded new right prices")

        Path(file_path).unlink(missing_ok=True)
    except FileNotFoundError:
        print("Right prices file doesn't exist")
