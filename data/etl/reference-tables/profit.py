from utils import load_files, clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load()
years_load = get_years_load()

files_load = [
    "itr_cia_aberta_DRE_ind_",
    "dfp_cia_aberta_DRE_ind_",
]

df = load_files(years_load, files_load)
df = clear_table(df, cd_cvm_load)

print(
    df[df["DS_CONTA"].str.contains("lucro") & df["DS_CONTA"].str.contains("prejuízo")]
    .groupby(["CD_CONTA", "DS_CONTA"])
    .count()
    .reset_index()
)

kpis = ["lucro ou prejuízo líquido do período", "lucro/prejuízo do período"]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-profit.csv", index=False)
