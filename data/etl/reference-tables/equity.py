### EQUITY (VALOR PATRIMONIAL)
from data.etl.utils import load_files
from utils import clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load()
years_load = get_years_load()

df = load_files(years_load, files_types_load=["BPP"])
df = clear_table(df, cd_cvm_load)

# print(
#     df[df["DS_CONTA"].str.contains("patrimônio")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = ["patrimônio líquido", "patrimônio líquido consolidado"]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-equity.csv", index=False)
