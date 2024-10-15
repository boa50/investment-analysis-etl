import pandas as pd
from data.etl.utils import (
    load_files,
    prepare_dataframe,
)
from profit import load_profit, load_cagr_profit_5_years
from earnings import load_ebit, load_ebitda
from equity import load_equity
from roe import load_roe
from debt import load_total_debt, load_net_debt, load_net_debt_by_ebitda


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
files_types_load = ["DRE"]

df_dre = load_files(years_load, files_types_load)
df_dre = prepare_dataframe(df_dre, cd_cvm_load)

df_profit = load_profit(df_dre, df_reference_table)
df_ebit = load_ebit(df_dre, df_reference_table)
df_ebitda = load_ebitda(df_dre, df_ebit, df_reference_table)
df_cagr_profit_5_years = load_cagr_profit_5_years(df_profit)

del df_dre

### KPIs from BPP files
files_types_load = ["BPP"]

df_bpp = load_files(years_load, files_types_load)
df_bpp["DT_INI_EXERC"] = "1900-01-01"
df_bpp = prepare_dataframe(df_bpp, cd_cvm_load)

df_equity = load_equity(df_bpp, df_reference_table)
df_total_debt = load_total_debt(df_bpp, df_reference_table)

del df_bpp

## KPIs from BPA files
files_types_load = ["BPA"]

df_bpa = load_files(years_load, files_types_load)
df_bpa["DT_INI_EXERC"] = "1900-01-01"
df_bpa = prepare_dataframe(df_bpa, cd_cvm_load)

df_net_debt = load_net_debt(df_bpa, df_total_debt, df_reference_table)

del df_bpa


### Consolidate the final file
df_roe = load_roe(df_profit, df_equity)
df_net_debt_by_ebitda = load_net_debt_by_ebitda(df_net_debt, df_ebitda)

df_fundaments = pd.concat(
    [
        df_profit,
        df_equity,
        df_roe,
        df_ebit,
        df_ebitda,
        df_total_debt,
        df_net_debt,
        df_net_debt_by_ebitda,
        df_cagr_profit_5_years,
    ]
)

print()
print("DF FUNDAMENTS")
print(df_fundaments.head())
print(df_fundaments.tail())
print()

df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)
