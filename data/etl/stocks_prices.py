import pandas as pd
import yfinance as yf
import time
import random


def get_main_ticker(tickers):
    tickers = tickers.split(";")

    for ticker in tickers:
        if ticker[4] == "4":
            return ticker

    for ticker in tickers:
        if ticker[4] == "3":
            return ticker

    return tickers[0]


def get_yf_ticker(tickers):
    ticker = get_main_ticker(tickers)
    return ticker + ".SA"


def get_prices(ticker):
    data = yf.Ticker(ticker)

    hist = data.history(period="10y", interval="1wk")
    hist = hist["Close"].reset_index()
    hist["Date"] = hist["Date"].dt.date
    hist.columns = ["DATE", "PRICE"]

    return hist


### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

df_prices = pd.DataFrame(columns=["CD_CVM", "TICKER", "DATE", "PRICE"])

for idx in range(df_basic_info.shape[0]):
    company = df_basic_info.iloc[idx]
    ticker = get_yf_ticker(company["TICKERS"])

    print(f"Getting prices for {ticker} CD_CVM: {company['CD_CVM']}")

    time.sleep(random.randint(1, 7))

    prices = get_prices(ticker)

    prices["TICKER"] = get_main_ticker(company["TICKERS"])
    prices["CD_CVM"] = company["CD_CVM"]

    df_prices = pd.concat([df_prices, prices])

print(df_prices.head())

df_prices.to_csv("data/processed/stocks-prices.csv", index=False)
