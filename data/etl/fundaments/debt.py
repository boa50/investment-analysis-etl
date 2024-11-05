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


def load_net_debt_by_ebitda(df_net_debt, df_ebitda, verbose=False):
    df_net_debt = df_net_debt.drop(
        ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
    )
    df_ebitda = df_ebitda.drop("VL_CONTA", axis=1)

    df_kpi = df_ebitda.merge(df_net_debt, how="left", on=["CD_CVM", "DT_FIM_EXERC"])

    df_kpi["NET_DEBT_BY_EBITDA"] = df_kpi["VL_CONTA"] / df_kpi["VL_CONTA_ROLLING_YEAR"]
    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1
    df_kpi["VL_CONTA"] = df_kpi["NET_DEBT_BY_EBITDA"]
    df_kpi["KPI"] = "NET_DEBT_BY_EBITDA"

    df_kpi = df_kpi.drop("NET_DEBT_BY_EBITDA", axis=1)

    if verbose:
        print()
        print("NET_DEBT_BY_EBITDA")
        print(df_kpi)
        print()

    return df_kpi


def load_net_debt_by_ebit(df_net_debt, df_ebit, verbose=False):
    kpi = "NET_DEBT_BY_EBIT"

    df_net_debt = df_net_debt.drop(
        ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
    )
    df_ebit = df_ebit.drop("VL_CONTA", axis=1)

    df_kpi = df_ebit.merge(df_net_debt, how="left", on=["CD_CVM", "DT_FIM_EXERC"])

    df_kpi[kpi] = df_kpi["VL_CONTA"] / df_kpi["VL_CONTA_ROLLING_YEAR"]
    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1
    df_kpi["VL_CONTA"] = df_kpi[kpi]
    df_kpi["KPI"] = kpi

    df_kpi = df_kpi.drop(kpi, axis=1)

    if verbose:
        print()
        print(kpi)
        print(df_kpi)
        print()

    return df_kpi


def load_net_debt_by_equity(df_net_debt, df_equity, verbose=False):
    kpi = "NET_DEBT_BY_EQUITY"

    df_net_debt = df_net_debt.drop(
        ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
    )
    df_equity = df_equity.drop("VL_CONTA_ROLLING_YEAR", axis=1)

    df_kpi = df_equity.merge(
        df_net_debt,
        how="left",
        on=["CD_CVM", "DT_FIM_EXERC"],
        suffixes=("_equity", "_debt"),
    )

    df_kpi[kpi] = df_kpi["VL_CONTA_debt"] / df_kpi["VL_CONTA_equity"]
    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1
    df_kpi["VL_CONTA"] = df_kpi[kpi]
    df_kpi["KPI"] = kpi

    df_kpi = df_kpi.drop([kpi, "VL_CONTA_debt", "VL_CONTA_equity"], axis=1)

    if verbose:
        print()
        print(kpi)
        print(df_kpi)
        print()

    return df_kpi
