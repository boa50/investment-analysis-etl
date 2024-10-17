import pandas as pd
from utils import get_dre_kpi_info


def load_profit(df_dre, df_reference_table, verbose=False):
    return get_dre_kpi_info("PROFIT", df_dre, df_reference_table, verbose=verbose)


def load_cagr_profit_5_years(df_profit, verbose=False):
    n_years = 5
    df_kpi = df_profit.copy()
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

    df_kpi["KPI"] = "CGAR_5_YEARS"
    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1

    df_kpi = df_kpi.drop(["VL_CONTA_ROLLING_YEAR_x", "VL_CONTA_ROLLING_YEAR_y"], axis=1)

    df_kpi = df_kpi.rename(
        columns={
            "DT_INI_EXERC_x": "DT_INI_EXERC",
            "DT_FIM_EXERC_x": "DT_FIM_EXERC",
            "EXERC_YEAR_x": "EXERC_YEAR",
        }
    )

    if verbose:
        print()
        print("CAGR PROFIT 5 YEARS")
        print(df_kpi.tail())
        print()
