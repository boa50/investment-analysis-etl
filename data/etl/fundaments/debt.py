from utils import get_bp_kpi_info


def load_total_debt(df_bpp, df_reference_table, verbose=False):
    return get_bp_kpi_info(
        "DEBT", df_bpp, df_reference_table, grouping=True, verbose=verbose
    )


def load_net_debt(df_bpa, df_total_debt, df_reference_table, verbose=False):
    df_kpi = get_bp_kpi_info(
        "DEBT-NEG", df_bpa, df_reference_table, grouping=True, verbose=verbose
    )

    df_kpi = df_kpi.drop(
        ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
    )

    df_kpi = df_kpi.merge(
        df_total_debt,
        how="left",
        on=["CD_CVM", "DT_FIM_EXERC"],
        suffixes=("_debt_neg", "_debt"),
    )

    df_kpi["VL_CONTA"] = df_kpi["VL_CONTA_debt"] - df_kpi["VL_CONTA_debt_neg"]
    df_kpi["KPI"] = "DEBT_NET"

    df_kpi = df_kpi.drop(
        [
            "VL_CONTA_debt",
            "VL_CONTA_debt_neg",
        ],
        axis=1,
    )

    df_kpi = df_kpi.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

    if verbose:
        print()
        print("DEBT_NET")
        print(df_kpi.head())
        print()

    return df_kpi
