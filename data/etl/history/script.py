import pandas as pd
import numpy as np
import queries as qu

df_basic_info = qu.get_basic_info()
df_prices = qu.get_prices()
df_dividends = qu.get_dividends()
df_fundaments = qu.get_fundaments()
df_stocks_available = qu.get_stocks_available()


### Function to include fundament data into the history dataframe
def include_fundament(df_history, df_fundaments, kpi_type, column_name):
    df_fundaments_filtered = df_fundaments[df_fundaments["KPI"] == kpi_type]

    df_history = df_history.merge(
        df_fundaments_filtered,
        how="left",
        left_on=["CD_CVM", "DATE"],
        right_on=["CD_CVM", "DT_END"],
    )
    df_history = df_history.drop(["KPI", "DT_END"], axis=1)
    fundament_columns = [column_name, column_name + "_ROLLING_YEAR"]

    df_history = df_history.rename(
        columns={
            "VALUE_FUNDAMENT": fundament_columns[0],
            "VALUE_ROLLING_YEAR": fundament_columns[1],
        }
    )

    df_history.loc[:, fundament_columns] = df_history.groupby("CD_CVM")[
        fundament_columns
    ].ffill()

    last_fundament_date = df_history.groupby("CD_CVM")["DATE"].min().reset_index()

    df_fundaments_filtered = df_fundaments_filtered.merge(
        last_fundament_date, how="left", on="CD_CVM"
    )
    df_fundaments_filtered = df_fundaments_filtered[
        df_fundaments_filtered["DT_END"] <= df_fundaments_filtered["DATE"]
    ]
    df_fundaments_filtered = (
        df_fundaments_filtered.groupby("CD_CVM")[
            ["VALUE_FUNDAMENT", "VALUE_ROLLING_YEAR"]
        ]
        .last()
        .reset_index()
    )

    df_history = df_history.merge(df_fundaments_filtered, how="left", on="CD_CVM")

    df_history = df_history.fillna(
        {
            fundament_columns[0]: df_history["VALUE_FUNDAMENT"],
            fundament_columns[1]: df_history["VALUE_ROLLING_YEAR"],
        }
    )

    df_history = df_history.drop(["VALUE_FUNDAMENT", "VALUE_ROLLING_YEAR"], axis=1)

    return df_history


### Creating the base structure based on stocks prices
df_history = pd.DataFrame()
df_dates = pd.DataFrame(
    {
        "DATE": pd.date_range(
            start=df_prices.iloc[0]["DATE"], end=df_prices.iloc[-1]["DATE"]
        )
    }
)

for ticker in df_prices["TICKER"].unique():
    df_prices_filtered = df_prices[df_prices["TICKER"] == ticker]

    df_tmp = df_dates.merge(df_prices_filtered, how="left", on="DATE")
    df_tmp = df_tmp.ffill()
    df_history = pd.concat([df_history, df_tmp])


df_history = df_history.merge(df_stocks_available, on="TICKER", how="left")

df_history = df_history.dropna(subset=["TICKER"])
df_history["CD_CVM"] = df_history["CD_CVM"].astype(int)


### Calculating the P/L
kpi_type = "PROFIT"
column_name = "PROFIT"

df_history = include_fundament(df_history, df_fundaments, kpi_type, column_name)

df_num_stocks = df_basic_info[["CD_CVM", "NUM_TOTAL"]]
df_history = df_history.merge(df_num_stocks, how="left", on="CD_CVM")

df_history["LPA"] = df_history["PROFIT_ROLLING_YEAR"] / df_history["NUM_TOTAL"]
df_history["PL"] = df_history["PRICE"] / df_history["LPA"]


### Calculating the Dividend Yield
df_history = pd.merge(
    df_history,
    df_dividends.groupby(["TICKER", "DATE"])["VALUE"].sum(),
    how="left",
    on=["TICKER", "DATE"],
)
df_history["VALUE"] = df_history["VALUE"].fillna(0)

df_dividends_rolling = (
    df_history.groupby("TICKER").rolling(window="365D", on="DATE")["VALUE"].sum()
)
df_dividends_rolling = df_dividends_rolling.reset_index()
df_dividends_rolling = df_dividends_rolling.rename(columns={"VALUE": "Dividends_1y"})

# Giving 30 days margin for not paying dividends on the same date of previous year
df_dividends_rolling["Dividends_1y"] = (
    df_dividends_rolling.replace(0, np.nan)
    .groupby("TICKER")["Dividends_1y"]
    .ffill(limit=30)
    .fillna(0)
)

df_history = pd.merge(
    df_history, df_dividends_rolling, how="left", on=["TICKER", "DATE"]
)

df_history["DIVIDEND_YIELD"] = df_history["Dividends_1y"] / df_history["PRICE"]


### Calculating the Dividend Payout
df_history["DIVIDEND_PAYOUT"] = df_history["Dividends_1y"] / df_history["LPA"]


### Calculating the PVP
kpi_type = "EQUITY"
column_name = "NET_WORTH"

df_history = include_fundament(df_history, df_fundaments, kpi_type, column_name)

df_history["VP"] = df_history["NET_WORTH"] / df_history["NUM_TOTAL"]
df_history["PVP"] = df_history["PRICE"] / df_history["VP"]


### Exporting the resulting dataset
df_history = df_history.drop(["PROFIT", "PROFIT_ROLLING_YEAR", "LPA"], axis=1)
df_history = df_history.drop(["VALUE", "Dividends_1y"], axis=1)
df_history = df_history.drop(["NET_WORTH", "NET_WORTH_ROLLING_YEAR", "VP"], axis=1)
df_history = df_history.drop(["NUM_TOTAL"], axis=1)

print(df_history.dtypes)
print(df_history)

# Keeping only weekly data
df_history = df_history.groupby(
    [
        # df_history["DATE"].dt.to_period("W-SUN"),
        df_history["DATE"].to_period("W-SUN"),
        df_history["CD_CVM"],
        df_history["TICKER"],
    ]
).head(1)

df_history.sort_values(by=["TICKER", "DATE"])

print()
print(df_history.head())
print(df_history.tail())
print()

df_history.to_csv("data/processed/stocks-history.csv", index=False)
