import os
import pandas as pd


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


def get_years_load():
    return [2019, 2020, 2021, 2022, 2023, 2024]


def get_cd_cvm_load():
    ### Getting only companies available on basic info file
    df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")
    return df_basic_info["CD_CVM"].values
