import pandas as pd
import datetime
import os.path


def load_files(years_load, files_load):
    df = pd.DataFrame()

    for year in years_load:
        for file in files_load:
            fname = "data/raw/" + file + str(year) + ".csv"

            if os.path.isfile(fname):
                df = pd.concat(
                    [
                        df,
                        pd.read_csv(
                            fname,
                            encoding="ISO-8859-1",
                            sep=";",
                        ),
                    ]
                )
            else:
                print(fname + " not found! Skipping")

    return df


def prepare_dataframe(df, cd_cvm_load):
    df = df[df["CD_CVM"].isin(cd_cvm_load)]
    df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]
    df = df[["CD_CVM", "DT_INI_EXERC", "DT_FIM_EXERC", "DS_CONTA", "VL_CONTA"]]
    df["DT_FIM_EXERC"] = pd.to_datetime(df["DT_FIM_EXERC"])
    df["DT_INI_EXERC"] = pd.to_datetime(df["DT_INI_EXERC"])
    df["EXERC_YEAR"] = df["DT_FIM_EXERC"].dt.year

    return df


df_fundaments = pd.DataFrame(
    columns=[
        "CD_CVM",
        "DT_INI_EXERC",
        "DT_FIM_EXERC",
        "DS_CONTA",
        "VL_CONTA",
        "EXERC_YEAR",
        "VL_CONTA_ROLLING_YEAR",
    ]
)

### Only 2022 forward became mandatory to share info with CVM
years_load = [2022, 2023, 2024]

### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
cd_cvm_load = df_basic_info["CD_CVM"].values


### COMPANY PROFIT
files_load = ["dfp_cia_aberta_DRE_con_", "itr_cia_aberta_DRE_con_"]

df = load_files(years_load, files_load)
df = prepare_dataframe(df, cd_cvm_load)

profit_raw = df[df["DS_CONTA"] == "Lucro ou Prejuízo Líquido Consolidado do Período"]

td_quarter = datetime.timedelta(days=93)
td_year = datetime.timedelta(days=360)

dt_diff = profit_raw["DT_FIM_EXERC"] - profit_raw["DT_INI_EXERC"]

profit_quarter = profit_raw[dt_diff <= td_quarter]
profit_quarter_grouped = (
    profit_quarter.groupby(["EXERC_YEAR", "CD_CVM"])
    .agg({"VL_CONTA": "sum", "DT_FIM_EXERC": "max"})
    .reset_index()
)

profit_year = profit_raw[dt_diff >= td_year]
profit_year = pd.merge(
    profit_year,
    profit_quarter_grouped,
    how="inner",
    on=["CD_CVM", "EXERC_YEAR"],
    suffixes=("_year", "_quarters"),
)
profit_year["VL_CONTA"] = (
    profit_year["VL_CONTA_year"] - profit_year["VL_CONTA_quarters"]
)
profit_year["DT_INI_EXERC"] = profit_year["DT_FIM_EXERC_quarters"] + datetime.timedelta(
    days=1
)
profit_year = profit_year.rename({"DT_FIM_EXERC_year": "DT_FIM_EXERC"}, axis=1)
profit_year = profit_year.drop(
    ["VL_CONTA_year", "VL_CONTA_quarters", "DT_FIM_EXERC_quarters"], axis=1
)

profit_quarter = (
    pd.concat([profit_quarter, profit_year])
    .sort_values(by=["DT_INI_EXERC", "CD_CVM"])
    .reset_index(drop=True)
)
profit_quarter["VL_CONTA"] = profit_quarter["VL_CONTA"] * 1000
profit_quarter["VL_CONTA_ROLLING_YEAR"] = profit_quarter.groupby("CD_CVM")[
    "VL_CONTA"
].transform(lambda s: s.rolling(4).sum())

profit_quarter["DS_CONTA"] = "Lucro ou Prejuízo"

print()
print("PROFIT")
print(profit_quarter.head())
print()


### PATRIMONIAL VALUE
files_load = ["dfp_cia_aberta_BPP_ind_", "itr_cia_aberta_BPP_ind_"]

df_pl = load_files(years_load, files_load)

df_pl = df_pl[df_pl["DS_CONTA"] == "Patrimônio Líquido"]
df_pl["DT_INI_EXERC"] = "1900-01-01"
df_pl = prepare_dataframe(df_pl, cd_cvm_load)

df_pl["VL_CONTA_ROLLING_YEAR"] = -1

df_pl = df_pl.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

print()
print("PL")
print(df_pl.head())
print()


### Consolidate final file
df_fundaments = pd.concat([profit_quarter, df_pl])

print()
print("DF FUNDAMENTS")
print(df_fundaments.head())
print()

df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)
