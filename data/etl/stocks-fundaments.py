import pandas as pd
from utils import load_files, prepare_dataframe, get_kpi_fields, transform_anual_quarter


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


### KPIs from DRE files
files_load = ["dfp_cia_aberta_DRE_ind_", "itr_cia_aberta_DRE_ind_"]

df_dre = load_files(years_load, files_load)
df_dre = prepare_dataframe(df_dre, cd_cvm_load)


def get_kpi_info(kpi):
    df_kpi = get_kpi_fields(df_dre, df_reference_table, kpi)
    df_kpi = transform_anual_quarter(df_kpi)

    print()
    print(kpi)
    print(df_kpi.tail(2))
    print()

    return df_kpi


###### PROFIT
def load_profit():
    return get_kpi_info("PROFIT")


###### EBIT
def load_ebit():
    return get_kpi_info("EBIT")


###### EBITDA
def load_ebitda():
    df_kpi = get_kpi_fields(df_dre, df_reference_table, "EBITDA-NEG")
    df_kpi = transform_anual_quarter(df_kpi)

    print()
    print("EBITDA")
    print(df_kpi.tail())
    print()

    return df_kpi


load_ebitda()

### KPIs from BPP files
files_load = ["dfp_cia_aberta_BPP_ind_", "itr_cia_aberta_BPP_ind_"]

df_bpp = load_files(years_load, files_load)
df_bpp["DT_INI_EXERC"] = "1900-01-01"
df_bpp = prepare_dataframe(df_bpp, cd_cvm_load)


###### EQUITY (VALOR PATRIMONIAL)
def load_equity():
    df_pl = get_kpi_fields(df_bpp, df_reference_table, "EQUITY")

    df_pl["VL_CONTA_ROLLING_YEAR"] = -1
    df_pl["VL_CONTA"] = df_pl["VL_CONTA"] * 1000

    df_pl = df_pl.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

    print()
    print("PL")
    print(df_pl.head(2))
    print()

    return df_pl


### OTHERS


###### ROE
def load_roe(df_profit, df_equity):
    df_profit = df_profit.drop(["KPI", "VL_CONTA"], axis=1)
    df_net_worth = df_equity.drop(
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
    print(df_roe.head(2))
    print()

    return df_roe


### Consolidate final file
# df_profit = load_profit()
# df_equity = load_equity()

# df_fundaments = pd.concat(
#     [df_profit, df_equity, load_roe(df_profit, df_equity), load_ebit()]
# )

# print()
# print("DF FUNDAMENTS")
# print(df_fundaments.head(2))
# print()

# df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)
