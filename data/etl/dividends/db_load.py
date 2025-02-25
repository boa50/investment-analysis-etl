from data.db_creation import batch_load
import queries
import pandas as pd
from pathlib import Path
import os


def load_dividends_into_db():
    try:
        df = pd.read_csv("data/processed/stocks-dividends.csv")

        df["TICKER_BASE"] = df["TICKER"].str[:4]

        df_docs_control = df[
            ["TICKER_BASE", "DOC_DATE", "DOC_VERSION"]
        ].drop_duplicates()

        for _, row in df_docs_control.iterrows():
            queries.delete_outdated_dividends(
                ticker_base=row["TICKER_BASE"],
                doc_date=row["DOC_DATE"],
                doc_version=row["DOC_VERSION"],
            )

        df = df.drop("TICKER_BASE", axis=1)

        batch_load.load_data(table_name="stocks-dividends", df=df)

        print()
        print("Loaded new dividends")

        Path("data/processed/stocks-dividends.csv").unlink(missing_ok=True)
    except FileNotFoundError:
        print("Dividend file doesn't exist")


def load_dividends_docs_into_db():
    try:
        df = pd.read_csv("data/processed/stocks-dividends-docs-processed.csv")
        batch_load.load_data(table_name="stocks-dividends-docs-processed", df=df)

        print()
        print("Loaded new dividends docs processed")

        Path("data/processed/stocks-dividends-docs-processed.csv").unlink(
            missing_ok=True
        )

        dividends_files_path = "data/raw/dividends/"
        downloaded_files = os.listdir(dividends_files_path)
        for file in downloaded_files:
            Path(dividends_files_path + file).unlink()

        Path(dividends_files_path).rmdir()
    except FileNotFoundError:
        print("Dividend docs processed file doesn't exist")


### CUSTOM DIVIDENDS (I STILL DON'T KNOW FROM WHERE THEY ARE COMMING)
def load_custom_dividends_into_db():
    def create_custom_dividend_row(ticker: str, date: str, value: float):
        return [ticker, date, value, "1900-01-01", -1]

    df_dividends_columns = ["TICKER", "DATE", "VALUE", "DOC_DATE", "VERSION"]

    df = pd.DataFrame(
        data=[
            create_custom_dividend_row("BBAS3", "2024-02-29", 0.00741179),
            create_custom_dividend_row("BBAS3", "2024-08-30", 0.00270692),
            create_custom_dividend_row("BBAS3", "2024-08-30", 0.00560564),
        ],
        columns=df_dividends_columns,
    )

    try:
        df_dividends = queries.get_all_custom_dividends()
    except Exception:
        df_dividends = pd.DataFrame(data=[], columns=df_dividends_columns)

    df_dividends["VALUE"] = df_dividends["VALUE"].astype(float)
    df_dividends["DATE"] = df_dividends["DATE"].astype(str)
    df_dividends["DOC_DATE"] = df_dividends["DOC_DATE"].astype(str)

    df_to_load = pd.merge(df, df_dividends, how="outer", indicator=True)
    df_to_load = df_to_load[df_to_load["_merge"] == "left_only"]
    df_to_load = df_to_load.drop("_merge", axis=1)

    if df_to_load.shape[0] > 0:
        print()
        print("Custom dividends to load")
        print(df_to_load)

        batch_load.load_data(table_name="stocks-dividends", df=df_to_load)
    else:
        print()
        print("No custom dividends to load")
