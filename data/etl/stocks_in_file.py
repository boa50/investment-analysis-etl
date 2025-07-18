from data.etl.utils import load_files
import data.etl.queries.queries as qu
from datetime import datetime


def create_stocks_files_list():
    years_load = list(range(2011, datetime.now().year + 1))

    files_types_load = ["BPA"]

    cd_cvm_load = [int(cd_cvm) for cd_cvm in qu.get_available_stocks()["CD_CVM"].values]

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

    df = df.sort_values(by=["CD_CVM", "FILE_CATEGORY"])

    # Fixing punctual problem where the CD_CVM 19348 doens't have all periods on the itr_con_2012 file
    df.loc[
        (df["CD_CVM"] == 19348) & (df["FILE_CATEGORY"] == "itr_con_2012"),
        "FILE_CATEGORY",
    ] = "itr_ind_2012"

    print(df)

    df.to_csv("data/processed/stocks-files.csv", index=False)
