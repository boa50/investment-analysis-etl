import pandas as pd
from utils import load_files


year_initial = 2011
year_final = 2024
years_load = list(range(year_initial, year_final + 1))

files_types_load = ["BPA"]

try:
    df_final = pd.read_csv("data/processed/stocks-files.csv")
except FileNotFoundError:
    df_final = pd.DataFrame()

### Getting only companies available on basic info file and that are not in the final file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

cd_cvm_load = list(
    set(df_basic_info["CD_CVM"].values).difference(df_final["CD_CVM"].values)
)


print(f"Checking stocks in files for CD_CVM: {cd_cvm_load}")

df = load_files(years_load, files_types_load)

df = df[df["CD_CVM"].isin(cd_cvm_load)]
df["FILE_CATEGORY"] = df["FILE_CATEGORY"].astype(str)
df[["FILE_PREFIX", "FILE_SUFIX", "FILE_YEAR"]] = df["FILE_CATEGORY"].str.split(
    "_", n=2, expand=True
)

df = (
    df.groupby(["CD_CVM", "FILE_PREFIX", "FILE_YEAR"])
    .agg({"FILE_CATEGORY": "min"})
    .reset_index()
)

df = df.drop(["FILE_PREFIX", "FILE_YEAR"], axis=1)

df_final = pd.concat([df_final, df])

df_final = df_final.sort_values(by=["CD_CVM", "FILE_CATEGORY"])

# Fixing punctual problem where the CD_CVM 19348 doens't have all periods on the itr_con_2012 file
df_final.loc[
    (df_final["CD_CVM"] == 19348) & (df_final["FILE_CATEGORY"] == "itr_con_2012"),
    "FILE_CATEGORY",
] = "itr_ind_2012"

print(df_final)

df_final.to_csv("data/processed/stocks-files.csv", index=False)
