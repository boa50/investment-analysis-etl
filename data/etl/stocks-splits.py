import pandas as pd
import yfinance as yf
from utils import get_yf_ticker

try:
    df_splits = pd.read_csv("data/processed/stocks-splits.csv")
except FileNotFoundError:
    df_splits = pd.DataFrame(columns=["CD_CVM", "DATE", "PROPORTION"])


### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
### Only updates
df_basic_info = df_basic_info[~df_basic_info["CD_CVM"].isin(df_splits["CD_CVM"].values)]

for idx in range(df_basic_info.shape[0]):
    company = df_basic_info.iloc[idx]

    print("Loading splits for CD_CVM: {}".format(company["CD_CVM"]))

    ticker = get_yf_ticker(company["TICKERS"])
    yf_ticker = yf.Ticker(ticker)

    hist = yf_ticker.history(period="10y")

    tk_splits = yf_ticker.splits
    tk_splits = tk_splits.reset_index()
    tk_splits.columns = ["DATE", "PROPORTION"]
    tk_splits["CD_CVM"] = company["CD_CVM"]
    tk_splits["DATE"] = tk_splits["DATE"].dt.date

    df_splits = pd.concat([df_splits, tk_splits])

### Not all displayed splits are real ones, must remove Bonificações
print(df_splits)

df_splits.to_csv("data/raw/stocks-splits-all.csv", index=False)
