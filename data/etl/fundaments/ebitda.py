from data.etl.utils import (
    get_kpi_fields,
    transform_anual_quarter,
)


def load_ebitda(df_ebit, df_dre, df_reference_table):
    df_kpi = get_kpi_fields(df_dre, df_reference_table, "EBITDA-NEG")
    df_kpi = (
        df_kpi.groupby(["CD_CVM", "DT_INI_EXERC", "DT_FIM_EXERC"])
        .agg({"KPI": "max", "VL_CONTA": "sum", "EXERC_YEAR": "max"})
        .reset_index()
    )
    df_kpi = transform_anual_quarter(df_kpi)

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

    print()
    print("EBITDA")
    print(df_kpi.tail(2))
    print()

    return df_kpi
