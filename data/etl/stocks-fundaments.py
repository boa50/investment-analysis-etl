import pandas as pd
import datetime
import os.path


def load_files(years_load, files_load):
    df = pd.DataFrame()

    for year in years_load:
        for file in files_load:
            fname = "data/raw/" + file + str(year) + ".csv"

            if os.path.isfile(fname):
                df_tmp = pd.read_csv(
                    fname,
                    encoding="ISO-8859-1",
                    sep=";",
                )

                df_tmp.loc[:, "FILE_NAME"] = file + str(year)

                df = pd.concat([df, df_tmp])
            else:
                print(fname + " not found! Skipping")

    return df


def prepare_dataframe(df, cd_cvm_load):
    df = df[df["CD_CVM"].isin(cd_cvm_load)]
    df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]
    df = df[
        [
            "CD_CVM",
            "DT_INI_EXERC",
            "DT_FIM_EXERC",
            "CD_CONTA",
            "DS_CONTA",
            "VL_CONTA",
            "FILE_NAME",
        ]
    ]
    df["DT_FIM_EXERC"] = pd.to_datetime(df["DT_FIM_EXERC"])
    df["DT_INI_EXERC"] = pd.to_datetime(df["DT_INI_EXERC"])
    df["EXERC_YEAR"] = df["DT_FIM_EXERC"].dt.year

    return df


def get_kpi_by_cvm_code(df, cd_cvm, kpi_name, file_names_loaded, df_reference_table):
    df_reference_table_tmp = df_reference_table[df_reference_table["CD_CVM"] == cd_cvm]

    general_cd_conta_value = df_reference_table_tmp[
        df_reference_table_tmp["FILE_NAME"] == "-1"
    ][["CD_CVM", "CD_CONTA"]]

    distinct_files = df_reference_table_tmp[df_reference_table_tmp["FILE_NAME"] != "-1"]
    distinct_files_names = distinct_files["FILE_NAME"].values

    distinct_files_cd_conta = distinct_files[distinct_files["CD_CONTA"] != "-1.0"][
        ["CD_CVM", "FILE_NAME", "CD_CONTA"]
    ]
    distinct_files_cd_conta["MATCHED_2"] = True

    distinct_files_ds_conta = distinct_files[distinct_files["DS_CONTA"] != "-1"][
        ["CD_CVM", "FILE_NAME", "DS_CONTA"]
    ]
    distinct_files_ds_conta["MATCHED_3"] = True

    general_cd_conta = pd.DataFrame()
    for fname in list(set(file_names_loaded).difference(distinct_files_names)):
        general_cd_conta = pd.concat(
            [
                general_cd_conta,
                pd.DataFrame(
                    {
                        "CD_CVM": general_cd_conta_value["CD_CVM"].iloc[0],
                        "FILE_NAME": fname,
                        "CD_CONTA": str(general_cd_conta_value["CD_CONTA"].iloc[0]),
                        "MATCHED_1": True,
                    },
                    index=[general_cd_conta.shape[0]],
                ),
            ]
        )

    df_kpi = df.merge(
        general_cd_conta, how="left", on=["CD_CVM", "FILE_NAME", "CD_CONTA"]
    )
    df_kpi = df_kpi.merge(
        distinct_files_cd_conta, how="left", on=["CD_CVM", "FILE_NAME", "CD_CONTA"]
    )
    df_kpi = df_kpi.merge(
        distinct_files_ds_conta, how="left", on=["CD_CVM", "FILE_NAME", "DS_CONTA"]
    )

    df_kpi = df_kpi[df_kpi[["MATCHED_1", "MATCHED_2", "MATCHED_3"]].any(axis=1)]
    df_kpi["KPI"] = kpi_name

    return df_kpi[
        [
            "CD_CVM",
            "DT_INI_EXERC",
            "DT_FIM_EXERC",
            "KPI",
            "VL_CONTA",
            "EXERC_YEAR",
        ]
    ]


def get_kpi_fields(df, df_reference_table, kpi_name):
    file_names_loaded = df["FILE_NAME"].unique()

    df_reference_table_tmp = df_reference_table[df_reference_table["KPI"] == kpi_name]
    df_reference_table_tmp["CD_CONTA"] = df_reference_table_tmp["CD_CONTA"].astype(str)

    df_kpi = pd.DataFrame()

    for cd_cvm in df_reference_table_tmp["CD_CVM"].unique():

        df_kpi = pd.concat(
            [
                df_kpi,
                get_kpi_by_cvm_code(
                    df, cd_cvm, kpi_name, file_names_loaded, df_reference_table_tmp
                ),
            ]
        )

    return df_kpi


df_fundaments = pd.DataFrame(
    columns=[
        "CD_CVM",
        "DT_INI_EXERC",
        "DT_FIM_EXERC",
        "KPI",
        "VL_CONTA",
        "EXERC_YEAR",
        "VL_CONTA_ROLLING_YEAR",
    ]
)

year_initial = 2019
year_final = 2021
years_load = list(range(year_initial, year_final + 1))

### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
cd_cvm_load = df_basic_info["CD_CVM"].values

### Getting the KPIs reference table
df_reference_table = pd.read_csv("data/processed/reference-table.csv")


### COMPANY PROFIT
files_load = ["dfp_cia_aberta_DRE_ind_", "itr_cia_aberta_DRE_ind_"]

df = load_files(years_load, files_load)
df = prepare_dataframe(df, cd_cvm_load)

profit_raw = get_kpi_fields(df, df_reference_table, "PROFIT")

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

print()
print("PROFIT")
print(profit_quarter.head())
print()


### EQUITY (VALOR PATRIMONIAL)
files_load = ["dfp_cia_aberta_BPP_ind_", "itr_cia_aberta_BPP_ind_"]

df_pl = load_files(years_load, files_load)
df_pl["DT_INI_EXERC"] = "1900-01-01"
df_pl = prepare_dataframe(df_pl, cd_cvm_load)

df_pl = get_kpi_fields(df_pl, df_reference_table, "EQUITY")

df_pl["VL_CONTA_ROLLING_YEAR"] = -1
df_pl["VL_CONTA"] = df_pl["VL_CONTA"] * 1000

df_pl = df_pl.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

print()
print("PL")
print(df_pl.head())
print()


### ROE
df_profit = profit_quarter.drop(["KPI", "VL_CONTA"], axis=1)
df_net_worth = df_pl.drop(
    ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
)

df_roe = df_profit.merge(df_net_worth, how="left", on=["CD_CVM", "DT_FIM_EXERC"])

df_roe["ROE"] = df_roe["VL_CONTA_ROLLING_YEAR"] / df_roe["VL_CONTA"]
df_roe["VL_CONTA_ROLLING_YEAR"] = -1
df_roe["VL_CONTA"] = df_roe["ROE"]
df_roe["KPI"] = "ROE"

df_roe = df_roe.drop("ROE", axis=1)

print()
print("ROE")
print(df_roe.head())
print()


### Consolidate final file
df_fundaments = pd.concat([profit_quarter, df_pl, df_roe])

print()
print("DF FUNDAMENTS")
print(df_fundaments.head())
print()

df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)
