import pandas as pd
import os
import datetime


def load_files(years_load, files_types_load):
    files_load = []

    files_prefixes = ["itr", "dfp"]
    files_sufixes = ["ind", "con"]
    for type in files_types_load:
        for prefix in files_prefixes:
            for sufix in files_sufixes:
                files_load.append(
                    {
                        "FILE_PREFIX": prefix,
                        "FILE_SUFIX": sufix,
                        "FILE_TYPE": type,
                        "FILE_NAME": prefix + "_cia_aberta_" + type + "_" + sufix + "_",
                    }
                )

    df = pd.DataFrame()

    for year in years_load:
        for file in files_load:
            fname = "data/raw/" + file["FILE_NAME"] + str(year) + ".csv"

            if os.path.isfile(fname):
                df_tmp = pd.read_csv(
                    fname,
                    encoding="ISO-8859-1",
                    sep=";",
                )

                df_tmp.loc[:, "FILE_PREFIX"] = file["FILE_PREFIX"]
                df_tmp.loc[:, "FILE_TYPE"] = file["FILE_TYPE"]
                df_tmp.loc[:, "FILE_SUFIX"] = file["FILE_SUFIX"]
                df_tmp.loc[:, "FILE_YEAR"] = str(year)
                # df_tmp.loc[:, "FILE_NAME"] = file["FILE_NAME"] + str(year)

                df = pd.concat([df, df_tmp])
            else:
                print(fname + " not found! Skipping")

    return df


def prepare_dataframe(df, cd_cvm_load):
    df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]
    df_stocks_files = pd.read_csv("data/processed/stocks-files.csv")
    df_stocks_files = df_stocks_files[df_stocks_files["CD_CVM"].isin(cd_cvm_load)]
    df_stocks_files["FILE_YEAR"] = df_stocks_files["FILE_YEAR"].astype(str)
    # df = df[df["CD_CVM"].isin(cd_cvm_load)]
    df = df.merge(
        df_stocks_files,
        how="inner",
        on=["CD_CVM", "FILE_PREFIX", "FILE_YEAR", "FILE_SUFIX"],
    )

    df = df[
        [
            "CD_CVM",
            "DT_INI_EXERC",
            "DT_FIM_EXERC",
            "CD_CONTA",
            "DS_CONTA",
            "VL_CONTA",
            "FILE_YEAR",
        ]
    ]
    df["DT_FIM_EXERC"] = pd.to_datetime(df["DT_FIM_EXERC"])
    df["DT_INI_EXERC"] = pd.to_datetime(df["DT_INI_EXERC"])
    # df["EXERC_YEAR"] = df["DT_FIM_EXERC"].dt.year
    df = df.rename(columns={"FILE_YEAR": "EXERC_YEAR"})

    return df


def get_kpi_by_cvm_code(df, cd_cvm, kpi_name, file_names_loaded, df_reference_table):
    df_reference_table_tmp = df_reference_table[df_reference_table["CD_CVM"] == cd_cvm]

    general_cd_conta_value = df_reference_table_tmp[
        df_reference_table_tmp["FILE_NAME"] == "-1"
    ][["CD_CVM", "CD_CONTA"]]

    distinct_files = df_reference_table_tmp[df_reference_table_tmp["FILE_NAME"] != "-1"]
    distinct_files_names = distinct_files["FILE_NAME"].values

    distinct_files_cd_conta = distinct_files[distinct_files["CD_CONTA"] != "-1.0"][
        ["CD_CVM", "FILE_NAME", "CD_CONTA"]
    ]
    distinct_files_cd_conta["MATCHED_2"] = True

    distinct_files_ds_conta = distinct_files[distinct_files["DS_CONTA"] != "-1"][
        ["CD_CVM", "FILE_NAME", "DS_CONTA"]
    ]
    distinct_files_ds_conta["MATCHED_3"] = True

    general_cd_conta = pd.DataFrame()
    for fname in list(set(file_names_loaded).difference(distinct_files_names)):
        for _, row in general_cd_conta_value.iterrows():
            general_cd_conta = pd.concat(
                [
                    general_cd_conta,
                    pd.DataFrame(
                        {
                            # "CD_CVM": general_cd_conta_value["CD_CVM"].iloc[0],
                            "CD_CVM": row["CD_CVM"],
                            "FILE_NAME": fname,
                            # "CD_CONTA": str(general_cd_conta_value["CD_CONTA"].iloc[0]),
                            "CD_CONTA": str(row["CD_CONTA"]),
                            "MATCHED_1": True,
                        },
                        index=[general_cd_conta.shape[0]],
                    ),
                ]
            )

    df_kpi = df.merge(
        general_cd_conta, how="left", on=["CD_CVM", "FILE_NAME", "CD_CONTA"]
    )
    df_kpi = df_kpi.merge(
        distinct_files_cd_conta, how="left", on=["CD_CVM", "FILE_NAME", "CD_CONTA"]
    )
    df_kpi = df_kpi.merge(
        distinct_files_ds_conta, how="left", on=["CD_CVM", "FILE_NAME", "DS_CONTA"]
    )

    df_kpi = df_kpi[df_kpi[["MATCHED_1", "MATCHED_2", "MATCHED_3"]].any(axis=1)]
    df_kpi["KPI"] = kpi_name

    return df_kpi[
        [
            "CD_CVM",
            "DT_INI_EXERC",
            "DT_FIM_EXERC",
            "KPI",
            "VL_CONTA",
            "EXERC_YEAR",
        ]
    ]


def get_kpi_fields(df, df_reference_table, kpi_name):
    file_names_loaded = df["FILE_NAME"].unique()

    df_reference_table_tmp = df_reference_table[df_reference_table["KPI"] == kpi_name]
    df_reference_table_tmp.loc[:, "CD_CONTA"] = df_reference_table_tmp[
        "CD_CONTA"
    ].astype(str)

    df_kpi = pd.DataFrame()

    for cd_cvm in df_reference_table_tmp["CD_CVM"].unique():
        df_kpi = pd.concat(
            [
                df_kpi,
                get_kpi_by_cvm_code(
                    df, cd_cvm, kpi_name, file_names_loaded, df_reference_table_tmp
                ),
            ]
        )

    return df_kpi


def transform_anual_quarter(df):
    td_quarter = datetime.timedelta(days=93)
    td_year = datetime.timedelta(days=360)

    dt_diff = df["DT_FIM_EXERC"] - df["DT_INI_EXERC"]

    df_quarter = df[dt_diff <= td_quarter]
    df_quarter_grouped = (
        df_quarter.groupby(["EXERC_YEAR", "CD_CVM"])
        .agg({"VL_CONTA": "sum", "DT_FIM_EXERC": "max"})
        .reset_index()
    )

    df_year = df[dt_diff >= td_year]
    df_year = pd.merge(
        df_year,
        df_quarter_grouped,
        how="inner",
        on=["CD_CVM", "EXERC_YEAR"],
        suffixes=("_year", "_quarters"),
    )
    df_year["VL_CONTA"] = df_year["VL_CONTA_year"] - df_year["VL_CONTA_quarters"]
    df_year["DT_INI_EXERC"] = df_year["DT_FIM_EXERC_quarters"] + datetime.timedelta(
        days=1
    )
    df_year = df_year.rename({"DT_FIM_EXERC_year": "DT_FIM_EXERC"}, axis=1)
    df_year = df_year.drop(
        ["VL_CONTA_year", "VL_CONTA_quarters", "DT_FIM_EXERC_quarters"], axis=1
    )

    df_quarter = (
        pd.concat([df_quarter, df_year])
        .sort_values(by=["DT_INI_EXERC", "CD_CVM"])
        .reset_index(drop=True)
    )
    df_quarter["VL_CONTA"] = df_quarter["VL_CONTA"] * 1000
    df_quarter["VL_CONTA_ROLLING_YEAR"] = df_quarter.groupby("CD_CVM")[
        "VL_CONTA"
    ].transform(lambda s: s.rolling(4).sum())

    return df_quarter
