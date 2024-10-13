from utils import get_dre_kpi_info


def load_ebit(df_dre, df_reference_table, verbose=False):
    return get_dre_kpi_info("EBIT", df_dre, df_reference_table, verbose=verbose)


def load_ebitda(df_dre, df_ebit, df_reference_table, verbose=False):
    df_kpi = get_dre_kpi_info(
        "EBITDA-NEG", df_dre, df_reference_table, grouping=True, verbose=verbose
    )

    df_kpi = df_kpi.drop(["KPI", "DT_INI_EXERC", "EXERC_YEAR"], axis=1)

    df_kpi = df_kpi.merge(
        df_ebit,
        how="left",
        on=["CD_CVM", "DT_FIM_EXERC"],
        suffixes=("_ebitda_neg", "_ebit"),
    )

    df_kpi["VL_CONTA"] = df_kpi["VL_CONTA_ebit"] - df_kpi["VL_CONTA_ebitda_neg"]
    df_kpi["VL_CONTA_ROLLING_YEAR"] = (
        df_kpi["VL_CONTA_ROLLING_YEAR_ebit"]
        - df_kpi["VL_CONTA_ROLLING_YEAR_ebitda_neg"]
    )
    df_kpi["KPI"] = "EBITDA"

    df_kpi = df_kpi.drop(
        [
            "VL_CONTA_ebit",
            "VL_CONTA_ebitda_neg",
            "VL_CONTA_ROLLING_YEAR_ebit",
            "VL_CONTA_ROLLING_YEAR_ebitda_neg",
        ],
        axis=1,
    )

    if verbose:
        print()
        print("EBITDA")
        print(df_kpi.tail())
        print()

    return df_kpi
