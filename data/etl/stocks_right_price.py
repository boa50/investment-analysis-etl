import pandas as pd


def calculate_bazin(constant=0.06, n_years=3):
    df_dividends = pd.read_csv(
        "data/processed/stocks-dividends.csv",
        usecols=["CD_CVM", "TICKER", "DATE", "VALUE"],
    )

    df_dividends["DATE"] = pd.to_datetime(df_dividends["DATE"])

    df_dividends_grorup = df_dividends.drop("VALUE", axis=1)
    df_dividends_grorup = (
        df_dividends_grorup.groupby(["CD_CVM", "TICKER"]).max().reset_index()
    )

    df_dividends_grorup["DATE_OFFSET"] = df_dividends["DATE"] - pd.DateOffset(
        years=n_years
    )
    df_dividends_grorup = df_dividends_grorup.drop("DATE", axis=1)

    df_bazin = df_dividends.merge(df_dividends_grorup, on=["CD_CVM", "TICKER"])

    df_bazin = df_bazin[df_bazin["DATE"] > df_bazin["DATE_OFFSET"]]

    df_bazin = df_bazin.drop(["DATE", "DATE_OFFSET"], axis=1)

    df_bazin = df_bazin.groupby(["CD_CVM", "TICKER"]).sum().reset_index()

    df_bazin["VALUE"] = df_bazin["VALUE"] / n_years / constant

    return df_bazin


df_prices = calculate_bazin()
df_prices = df_prices.rename(columns={"VALUE": "BAZIN"})

print(df_prices.head())

df_prices.to_csv("data/processed/stocks-right-prices.csv", index=False)
