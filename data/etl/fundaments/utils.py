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
