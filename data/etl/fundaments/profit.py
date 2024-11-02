import pandas as pd
from utils import get_dre_kpi_info, get_cgar


def load_profit(df_dre, df_reference_table, verbose=False):
    return get_dre_kpi_info("PROFIT", df_dre, df_reference_table, verbose=verbose)


def load_cagr_profit_5_years(df_profit, verbose=False):
    df_kpi = get_cgar(df_profit)
    df_kpi["KPI"] = "CGAR_5_YEARS_PROFIT"

    if verbose:
        print()
        print("CAGR PROFIT 5 YEARS")
        print(df_kpi.tail())
        print()

    return df_kpi


def load_net_margin(df_profit, df_net_revenue, verbose=False):
    df_profit = df_profit.drop("KPI", axis=1)
    df_net_revenue = df_net_revenue.drop(["DT_INI_EXERC", "KPI", "EXERC_YEAR"], axis=1)

    df_kpi = pd.merge(
        df_profit,
        df_net_revenue,
        on=["CD_CVM", "DT_FIM_EXERC"],
        suffixes=("_profit", "_revenue"),
    )
    df_kpi["VL_CONTA"] = df_kpi["VL_CONTA_profit"] / df_kpi["VL_CONTA_revenue"]
    df_kpi["VL_CONTA_ROLLING_YEAR"] = (
        df_kpi["VL_CONTA_ROLLING_YEAR_profit"] / df_kpi["VL_CONTA_ROLLING_YEAR_revenue"]
    )
    df_kpi = df_kpi.drop(
        [
            "VL_CONTA_profit",
            "VL_CONTA_revenue",
            "VL_CONTA_ROLLING_YEAR_profit",
            "VL_CONTA_ROLLING_YEAR_revenue",
        ],
        axis=1,
    )
    df_kpi["KPI"] = "NET_MARGIN"

    if verbose:
        print()
        print("NET MARGIN")
        print(df_kpi.tail())
        print()

    return df_kpi
