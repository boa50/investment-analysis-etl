import pandas as pd
from data.etl.utils import (
    get_kpi_fields,
    transform_anual_quarter,
)


def get_dre_kpi_info(kpi, df_dre, df_reference_table, grouping=False, verbose=False):
    df_kpi = get_kpi_fields(df_dre, df_reference_table, kpi)

    if grouping:
        df_kpi = (
            df_kpi.groupby(["CD_CVM", "DT_INI_EXERC", "DT_FIM_EXERC"])
            .agg({"KPI": "max", "VL_CONTA": "sum", "EXERC_YEAR": "max"})
            .reset_index()
        )

    df_kpi = transform_anual_quarter(df_kpi)

    if verbose:
        print()
        print(kpi)
        print(df_kpi.tail())
        print()

    return df_kpi


def get_bp_kpi_info(kpi, df_bp, df_reference_table, grouping=False, verbose=False):
    df_kpi = get_kpi_fields(df_bp, df_reference_table, kpi)

    if grouping:
        df_kpi = (
            df_kpi.groupby(["CD_CVM", "DT_INI_EXERC", "DT_FIM_EXERC"])
            .agg({"KPI": "max", "VL_CONTA": "sum", "EXERC_YEAR": "max"})
            .reset_index()
        )

    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1
    df_kpi["VL_CONTA"] = df_kpi["VL_CONTA"] * 1000

    df_kpi = df_kpi.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

    if verbose:
        print()
        print(kpi)
        print(df_kpi.head())
        print()

    return df_kpi


def get_cagr(df_base, n_years=5):
    df_kpi = df_base.copy()
    df_kpi = df_kpi.drop(["KPI", "VL_CONTA"], axis=1)
    df_kpi["DT_FIM_EXERC_MINUS_ONE_YEAR"] = df_kpi["DT_FIM_EXERC"] - pd.DateOffset(
        years=n_years
    )

    df_kpi = df_kpi.merge(
        df_kpi,
        how="left",
        left_on=["CD_CVM", "DT_FIM_EXERC_MINUS_ONE_YEAR"],
        right_on=["CD_CVM", "DT_FIM_EXERC"],
    )

    df_kpi = df_kpi.drop(
        [
            "DT_FIM_EXERC_MINUS_ONE_YEAR_x",
            "DT_INI_EXERC_y",
            "DT_FIM_EXERC_y",
            "EXERC_YEAR_y",
            "DT_FIM_EXERC_MINUS_ONE_YEAR_y",
        ],
        axis=1,
    )

    df_kpi["VL_CONTA"] = (
        df_kpi["VL_CONTA_ROLLING_YEAR_x"] / df_kpi["VL_CONTA_ROLLING_YEAR_y"]
    ) ** (1 / n_years) - 1

    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1

    df_kpi = df_kpi.drop(["VL_CONTA_ROLLING_YEAR_x", "VL_CONTA_ROLLING_YEAR_y"], axis=1)

    df_kpi = df_kpi.rename(
        columns={
            "DT_INI_EXERC_x": "DT_INI_EXERC",
            "DT_FIM_EXERC_x": "DT_FIM_EXERC",
            "EXERC_YEAR_x": "EXERC_YEAR",
        }
    )

    return df_kpi
