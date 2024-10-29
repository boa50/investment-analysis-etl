from data.etl.utils import load_files
from utils import clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load(kpi="EBIT")
years_load = get_years_load()

print(f"Filling reference table for {cd_cvm_load}")

df = load_files(years_load, files_types_load=["DRE"])
df = clear_table(df, cd_cvm_load)

# print(
#     df[df["DS_CONTA"].str.contains("resultado antes")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = [
    "resultado antes do resultado financeiro e dos tributos",
    "resultado antes dos tributos sobre o lucro",
    "resultado antes tributação/participações",
]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-ebit.csv", index=False)
