import pandas as pd
from data.etl.utils import (
    load_files,
    prepare_dataframe,
)
from profit import load_profit
from ebit import load_ebit
from ebitda import load_ebitda
from equity import load_equity
from roe import load_roe
from debt import load_total_debt


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


### KPIs from BPP files
files_load = ["dfp_cia_aberta_BPP_ind_", "itr_cia_aberta_BPP_ind_"]

df_bpp = load_files(years_load, files_load)
df_bpp["DT_INI_EXERC"] = "1900-01-01"
df_bpp = prepare_dataframe(df_bpp, cd_cvm_load)


### Consolidate final file
df_profit = load_profit(df_dre, df_reference_table)
df_equity = load_equity(df_bpp, df_reference_table)
df_ebit = load_ebit(df_dre, df_reference_table)


df_fundaments = pd.concat(
    [
        df_profit,
        df_equity,
        load_roe(df_profit, df_equity),
        df_ebit,
        load_ebitda(df_ebit, df_dre, df_reference_table),
        load_total_debt(df_bpp, df_reference_table),
    ]
)

print()
print("DF FUNDAMENTS")
print(df_fundaments.head(2))
print(df_fundaments.tail(2))
print()

df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)
