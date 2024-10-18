import pandas as pd
from data.etl.scrapping.b3 import load_dividends


def get_main_ticker(tickers):
    tickers = tickers.split(";")

    for ticker in tickers:
        if ticker[4] == "4":
            return ticker

    for ticker in tickers:
        if ticker[4] == "3":
            return ticker

    return tickers[0]


try:
    df_dividends = pd.read_csv("data/processed/stocks-dividends.csv")
except FileNotFoundError:
    df_dividends = pd.DataFrame(columns=["CD_CVM", "TICKER", "DATE", "VALUE", "TYPE"])

### Getting data related to stocks splitting
df_splits = pd.read_csv("data/processed/stocks-splits.csv")
df_splits["DATE"] = pd.to_datetime(df_splits["DATE"])


### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
### Only updates
df_basic_info = df_basic_info[
    ~df_basic_info["CD_CVM"].isin(df_dividends["CD_CVM"].values)
]

for idx in range(df_basic_info.shape[0]):
    company = df_basic_info.iloc[idx]
    ticker = get_main_ticker(company["TICKERS"])

    # tk_dividends = load_dividends(company["CD_CVM"], ticker[:4])
    tk_dividends = pd.read_csv("data/raw/_test-bbas-dividends.csv")
    tk_dividends["DATE"] = pd.to_datetime(
        tk_dividends["DATE"], format="%d/%m/%Y"
    ).dt.date
    tk_dividends["TICKER"] = ticker
    tk_dividends["CD_CVM"] = company["CD_CVM"]

    df_splits_tk = df_splits[df_splits["CD_CVM"] == company["CD_CVM"]]
    for _, row in df_splits_tk.iterrows():
        tk_dividends.loc[tk_dividends["DATE"] <= row["DATE"], "VALUE"] /= row[
            "PROPORTION"
        ]

    df_dividends = pd.concat([df_dividends, tk_dividends])

print(df_dividends.tail())

df_dividends.to_csv("data/processed/stocks-dividends-test.csv", index=False)
