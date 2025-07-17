import pandas as pd
import data.etl.prices.queries as queries
from datetime import datetime
import yfinance as yf
import time
import random


def _get_prices(ticker: str, days_diff: int):
    data = yf.Ticker(ticker)

    hist = data.history(period=f"{days_diff + 1}d", interval="1wk")

    hist = hist["Close"].reset_index()
    hist["Date"] = hist["Date"].dt.date
    hist.columns = ["DATE", "PRICE"]

    return hist


def get_latest_prices():
    last_update = queries.get_last_load_date()
    today_date = datetime.now().date()
    days_diff = (today_date - last_update).days

    df_prices = pd.DataFrame(columns=["TICKER", "DATE", "PRICE"])

    available_tickers = queries.get_available_tickers()

    for ticker in available_tickers:
        ticker_yf = f"{ticker}.SA"

        print(f"Getting prices for {ticker}")

        time.sleep(random.randint(1, 7))

        prices = _get_prices(ticker_yf, days_diff=days_diff)

        prices["TICKER"] = ticker

        df_prices = pd.concat([df_prices, prices])

    df_prices = df_prices[df_prices["DATE"] >= last_update]

    df_prices.to_csv("data/processed/stocks-prices.csv", index=False)
