import pandas as pd
import os


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


def clear_table(df, cd_cvm_load):
    df_fields = df.copy()
    df_fields = df_fields[df_fields["CD_CVM"].isin(cd_cvm_load)]
    df_fields["DS_CONTA"] = df_fields["DS_CONTA"].str.lower()
    df_fields = (
        df_fields.groupby(["CD_CVM", "FILE_NAME", "CD_CONTA", "DS_CONTA"])
        .count()
        .reset_index()
    )
    df_fields = df_fields[["CD_CVM", "FILE_NAME", "CD_CONTA", "DS_CONTA"]]

    return df_fields


years_load = [2019, 2020, 2021, 2022, 2023, 2024]

### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
cd_cvm_load = df_basic_info["CD_CVM"].values

### PROFIT
# files_load = [
#     "itr_cia_aberta_DRE_ind_",
#     "dfp_cia_aberta_DRE_ind_",
# ]

# df = load_files(years_load, files_load)
# df = clear_table(df, cd_cvm_load)

# print(
#     df[df["DS_CONTA"].str.contains("lucro") & df["DS_CONTA"].str.contains("prejuízo")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

# kpis = ["lucro ou prejuízo líquido do período", "lucro/prejuízo do período"]
# df = df[df["DS_CONTA"].isin(kpis)]
# print(df)
# df.to_csv("data/raw/_reference-table-profit.csv", index=False)


### EQUITY (VALOR PATRIMONIAL)
# files_load = [
#     "itr_cia_aberta_BPP_ind_",
#     "dfp_cia_aberta_BPP_ind_",
# ]

# df = load_files(years_load, files_load)
# df = clear_table(df, cd_cvm_load)

# print(
#     df[df["DS_CONTA"].str.contains("patrimônio")]
#     .groupby(["CD_CONTA", "DS_CONTA"])
#     .count()
#     .reset_index()
# )

# kpis = ["patrimônio líquido"]
# df = df[df["DS_CONTA"].isin(kpis)]
# print(df)
# df.to_csv("data/raw/_reference-table-equity.csv", index=False)


### EBIT
files_load = [
    "itr_cia_aberta_DRE_ind_",
    "dfp_cia_aberta_DRE_ind_",
]

df = load_files(years_load, files_load)
df = clear_table(df, cd_cvm_load)

print(
    df[df["DS_CONTA"].str.contains("resultado antes")]
    .groupby(["CD_CONTA", "DS_CONTA"])
    .count()
    .reset_index()
)

kpis = [
    "resultado antes do resultado financeiro e dos tributos",
    "resultado antes dos tributos sobre o lucro",
    "resultado antes tributação/participações",
]
df = df[df["DS_CONTA"].isin(kpis)]
print(df)
df.to_csv("data/raw/_reference-table-ebit.csv", index=False)
