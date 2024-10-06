import pandas as pd

df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
df_prices = pd.read_csv("data/processed/stocks-prices.csv", parse_dates=["DATE"])
df_dividends = pd.read_csv("data/processed/stocks-dividends.csv", parse_dates=["DATE"])
df_fundaments = pd.read_csv(
    "data/processed/stocks-fundaments.csv",
    parse_dates=["DT_INI_EXERC", "DT_FIM_EXERC"],
)


### Function to include fundament data into the history dataframe
def include_fundament(df_history, df_fundaments, fundament_type, column_name):
    df_fundaments_filtered = df_fundaments[df_fundaments["DS_CONTA"] == fundament_type]
    df_history = df_history.merge(
        df_fundaments_filtered,
        how="left",
        left_on=["CD_CVM", "DATE"],
        right_on=["CD_CVM", "DT_FIM_EXERC"],
    )
    df_history = df_history.drop(
        ["DS_CONTA", "DT_INI_EXERC", "DT_FIM_EXERC", "EXERC_YEAR"], axis=1
    )
    fundament_columns = [column_name, column_name + "_ROLLING_YEAR"]

    df_history = df_history.rename(
        columns={
            "VL_CONTA": fundament_columns[0],
            "VL_CONTA_ROLLING_YEAR": fundament_columns[1],
        }
    )

    df_history.loc[:, fundament_columns] = df_history.groupby("CD_CVM")[
        fundament_columns
    ].ffill()

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


### Calculating the P/L
fundament_type = "Lucro ou Prejuízo"
column_name = "PROFIT"

df_history = include_fundament(df_history, df_fundaments, fundament_type, column_name)

df_num_stocks = df_basic_info[["CD_CVM", "NUM_TOTAL"]]
df_history = df_history.merge(df_num_stocks, how="left", on="CD_CVM")

df_history["LPA"] = df_history["PROFIT_ROLLING_YEAR"] / df_history["NUM_TOTAL"]
df_history["PL"] = df_history["PRICE"] / df_history["LPA"]

df_history = df_history.drop(
    ["PROFIT", "PROFIT_ROLLING_YEAR", "NUM_TOTAL", "LPA"], axis=1
)

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

df_history = pd.merge(
    df_history, df_dividends_rolling, how="left", on=["TICKER", "DATE"]
)

df_history["DIVIDEND_YIELD"] = df_history["Dividends_1y"] / df_history["PRICE"]

df_history = df_history.drop(["VALUE", "Dividends_1y"], axis=1)


### Exporting the resulting dataset
print(df_history)

df_history.to_csv("data/processed/stocks-history.csv", index=False)
