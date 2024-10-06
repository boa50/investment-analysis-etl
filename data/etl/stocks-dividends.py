import pandas as pd
from bs4 import BeautifulSoup
import requests


def get_main_ticker(tickers):
    tickers = tickers.split(";")

    for ticker in tickers:
        if ticker[4] == "4":
            return ticker

    for ticker in tickers:
        if ticker[4] == "3":
            return ticker

    return tickers[0]


def get_dividends(ticker):
    url = "https://www.dadosdemercado.com.br/acoes/" + ticker + "/dividendos"
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    cells = soup.find(class_="table-container").find_all("td")

    df_dividends = pd.DataFrame()

    for row_idx in range(int(len(cells) / 5)):
        row_start = row_idx * 5
        row_end = row_start + 5
        row = cells[row_start:row_end]

        dividend = pd.DataFrame(
            {
                "DATE": row[2].text,
                "VALUE": float(row[1].text.replace("*", "").replace(",", ".")),
                "TYPE": row[0].text,
            },
            index=[df_dividends.shape[0]],
        )

        df_dividends = pd.concat([df_dividends, dividend])

    df_dividends["DATE"] = pd.to_datetime(df_dividends["DATE"], format="%d/%m/%Y")

    return df_dividends


df_dividends = pd.DataFrame(columns=["CD_CVM", "TICKER", "DATE", "VALUE", "TYPE"])

### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

for idx in range(df_basic_info.shape[0]):
    company = df_basic_info.iloc[idx]
    ticker = get_main_ticker(company["TICKERS"])

    tk_dividends = get_dividends(ticker)
    tk_dividends["TICKER"] = ticker
    tk_dividends["CD_CVM"] = company["CD_CVM"]

    df_dividends = pd.concat([df_dividends, tk_dividends])

print(df_dividends.head())

df_dividends.to_csv("data/processed/stocks-dividends.csv", index=False)
