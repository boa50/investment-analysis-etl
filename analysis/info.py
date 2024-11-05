import pandas as pd

df_basic_info = pd.read_csv("../data/processed/stocks-basic-info.csv")


def get_main_ticker(tickers):
    tickers = tickers.split(";")

    for ticker in tickers:
        if ticker[4] == "4":
            return ticker

    for ticker in tickers:
        if ticker[4] == "3":
            return ticker

    return tickers[0]


def get_sectors():
    return df_basic_info[["SETOR", "SUBSETOR", "SEGMENTO"]].drop_duplicates()


def get_company_by_ticker(ticker):
    return df_basic_info[df_basic_info["TICKERS"].str.contains(ticker)].iloc[0]


def get_segmento_by_ticker(ticker):
    return get_company_by_ticker(ticker)["SEGMENTO"]


def get_cd_cvm_by_ticker(ticker):
    return get_company_by_ticker(ticker)["CD_CVM"]


def get_companies_by_segmento(segmento):
    df_tmp = df_basic_info[df_basic_info["SEGMENTO"] == segmento].copy()
    df_tmp["MAIN_TICKER"] = df_tmp["TICKERS"].apply(get_main_ticker)
    return df_tmp[["CD_CVM", "DENOM_COMERC", "MAIN_TICKER", "FOUNDATION"]]
