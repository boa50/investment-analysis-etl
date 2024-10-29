import pandas as pd
from data.etl.utils import load_files
from utils import clear_table, get_cd_cvm_load, get_years_load

cd_cvm_load = get_cd_cvm_load(kpi="EBITDA-NEG")
years_load = get_years_load()

print(f"Filling reference table for {cd_cvm_load}")

df = load_files(years_load, files_types_load=["DRE"])
df = clear_table(df, cd_cvm_load)


### Negative values
# print(
#     df[df["DS_CONTA"].str.contains("equivalência patrimonial")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

# kpis = ["resultado de equivalência patrimonial"]
# df_general = df[df["DS_CONTA"].isin(kpis)]

# df_case01 = df[df["CD_CVM"].isin([18376])]
df_case01 = df.copy()

# print(
#     df_case01[df_case01["DS_CONTA"].str.contains("outras receitas")]
#     # df_case01[df_case01["DS_CONTA"].str.contains("outras despesas")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = ["outras receitas operacionais", "outras despesas operacionais"]
df_case01 = df_case01[df_case01["DS_CONTA"].isin(kpis)]


# df_case02 = df[df["CD_CVM"].isin([20257])]
df_case02 = df.copy()

# print(
#     df_case02[df_case02["DS_CONTA"].str.contains("depreciação")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

kpis = ["depreciação e amortização"]
df_case02 = df_case02[df_case02["DS_CONTA"].isin(kpis)]

# df = pd.concat([df_general, df_case01, df_case02])
df = pd.concat([df_case01, df_case02])
print(df)
df.to_csv("data/raw/_reference-table-ebitda-neg.csv", index=False)
