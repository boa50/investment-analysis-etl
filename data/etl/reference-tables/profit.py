from data.etl.utils import load_files
from utils import clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load(kpi="PROFIT")
years_load = get_years_load()

print(f"Filling reference table for {cd_cvm_load}")

df = load_files(years_load, files_types_load=["DRE"])
df = clear_table(df, cd_cvm_load)

# print(
#     df[df["DS_CONTA"].str.contains("lucro") & df["DS_CONTA"].str.contains("prejuízo")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = [
    "lucro ou prejuízo líquido do período",
    "lucro/prejuízo do período",
    "lucro/prejuízo consolidado do período",
    "lucro ou prejuízo líquido consolidado do período",
]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-profit.csv", index=False)
