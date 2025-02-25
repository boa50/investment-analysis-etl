import pandas as pd
import numpy as np
import os
import utils


def clean_unused_tickers(df):
    df["TICKER"] = np.where(
        (df["TICKER_BASE"] == "BPAC") & (df["STOCK_TYPE"] == "UNT"),
        "BPAC11",
        df["TICKER"],
    )

    def remove_ticker(ticker_base, type="PN"):
        df["TICKER"] = np.where(
            (df["TICKER_BASE"] == ticker_base) & (df["STOCK_TYPE"].str[:2] == type),
            "-1",
            df["TICKER"],
        )

    remove_ticker("PNVL")
    remove_ticker("BBAS")
    remove_ticker("NEOE")
    remove_ticker("EQTL")
    remove_ticker("EQTL", type="UN")
    remove_ticker("RADL")
    remove_ticker("EGIE")

    df = df[df["TICKER"] != "-1"]

    return df


df_dividends = pd.DataFrame()

files_path = "data/raw/"
files = [f for f in os.listdir(files_path) if "_dividends_" in f]

df_code_ticker = pd.read_csv(
    "data/raw/b3_stocks_codes_tickers.csv",
    encoding="ISO-8859-1",
    skiprows=1,
    sep=";",
    usecols=["TckrSymb", "ISIN"],
)

df_code_ticker.columns = ["TICKER", "ISIN"]
df_code_ticker = df_code_ticker[
    df_code_ticker["TICKER"].str.len().isin([5, 6])
    & ~df_code_ticker["TICKER"].str[-1].isin(["F", "M", "Q", "R"])
]

df_code_ticker["TICKER_BASE"] = df_code_ticker["TICKER"].str[:4]
df_code_ticker["STOCK_TYPE"] = df_code_ticker["ISIN"].str[-3:-1]


def map_ticker_type(tk_type):
    ticker_type_mapping = {
        "OR": "ON",
        "PR": "PN",
        "M1": "UNT",
        "PA": "PNA",
        "PB": "PNB",
    }

    try:
        return ticker_type_mapping[tk_type]
    except Exception:
        return "-"


df_code_ticker["STOCK_TYPE"] = [
    map_ticker_type(tk_type) for tk_type in df_code_ticker["STOCK_TYPE"]
]

for file in files:
    ticker_base = file[-8:-4]
    print(f"Organising dividends for {ticker_base}")

    df_dividends_tk = pd.read_csv(os.path.join(files_path, file))

    df_dividends_tk["TICKER_BASE"] = ticker_base

    df_dividends_tk["DATE"] = pd.to_datetime(df_dividends_tk["DATE"], format="%d/%m/%Y")

    df_dividends_tk = pd.merge(
        df_dividends_tk, df_code_ticker, on=["TICKER_BASE", "STOCK_TYPE"], how="left"
    )

    df_dividends = pd.concat([df_dividends, df_dividends_tk])

df_dividends = clean_unused_tickers(df_dividends)

df_dividends = df_dividends[["TICKER", "DATE", "VALUE", "TYPE"]]

df_dividends = utils.calculate_value_splits(df=df_dividends)

df_dividends.to_csv("data/processed/stocks-dividends-old.csv", index=False)
