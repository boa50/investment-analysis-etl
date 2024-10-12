import pandas as pd
from utils import load_files


year_initial = 2019
year_final = 2024
years_load = list(range(year_initial, year_final + 1))

files_types_load = ["BPA"]

df = load_files(years_load, files_types_load)

### Getting only companies available on basic info file
df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

df = df[df["CD_CVM"].isin(df_basic_info["CD_CVM"].values)]

df = (
    df.groupby(["CD_CVM", "FILE_PREFIX", "FILE_YEAR"])
    .agg({"FILE_SUFIX": "min"})
    .reset_index()
)

df.to_csv("data/processed/stocks-files.csv", index=False)
