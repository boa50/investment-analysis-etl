import pandas as pd
import data.etl.dividends.queries as queries


def calculate_value_splits(df: pd.DataFrame):
    df_all_dividends = pd.DataFrame()
    tickers = df["TICKER"].unique()
    stocks_splits = queries.get_stocks_splits()
    stocks_splits["DATE"] = pd.to_datetime(stocks_splits["DATE"])
    stocks_splits["PROPORTION"] = stocks_splits["PROPORTION"].astype(float)

    df_cds_cvm = queries.get_cds_cvm()
    df_cds_cvm["TICKER_BASE"] = df_cds_cvm["TICKERS"].str[:4]
    df_cds_cvm = df_cds_cvm.drop("TICKERS", axis=1)

    for ticker in tickers:
        cd_cvm = df_cds_cvm[df_cds_cvm["TICKER_BASE"] == ticker[:4]].iat[0, 0]
        stocks_splits_tk = stocks_splits[stocks_splits["CD_CVM"] == cd_cvm]
        df_dividends_tk = df[df["TICKER"] == ticker]

        for _, row in stocks_splits_tk.iterrows():
            df_dividends_tk.loc[df_dividends_tk["DATE"] <= row["DATE"], "VALUE"] /= row[
                "PROPORTION"
            ]

        df_all_dividends = pd.concat([df_all_dividends, df_dividends_tk])

    return df_all_dividends
