from data.etl.utils import load_files
from utils import clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load()
years_load = get_years_load()

df = load_files(years_load, files_types_load=["BPP"])

# Banks don't have Debt
# cd_cvm_load_excluding_banks = list(set(cd_cvm_load).difference([906, 1023]))
# df = clear_table(df, cd_cvm_load_excluding_banks)

# # print(
# #     df[df["DS_CONTA"].str.contains("empréstimos e financiamentos")]
# #     .groupby(["CD_CONTA", "DS_CONTA"])
# #     .count()
# #     .reset_index()
# # )

# kpis = [
#     "empréstimos e financiamentos",
# ]
# df = df[df["DS_CONTA"].isin(kpis)]
# df = df[df["CD_CONTA"].str.len() < 10]
# print(df)
# df.to_csv("data/raw/_reference-table-debt.csv", index=False)


### Cash equivalents to calculate the Net Debt
df = load_files(years_load, files_types_load=["BPA"])

# Banks don't have Debt
cd_cvm_load_excluding_banks = list(set(cd_cvm_load).difference([906, 1023]))
df = clear_table(df, cd_cvm_load_excluding_banks)

# print(
#     df[df["DS_CONTA"].str.contains("empréstimos e financiamentos")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = [
    "caixa e equivalentes de caixa",
    "títulos e valores mobiliários",
    "aplicações financeiras",
]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-debt-neg.csv", index=False)
