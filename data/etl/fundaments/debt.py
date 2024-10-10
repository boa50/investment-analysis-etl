from data.etl.utils import (
    get_kpi_fields,
)


def load_total_debt(df_bpp, df_reference_table):
    df_kpi = get_kpi_fields(df_bpp, df_reference_table, "DEBT")

    df_kpi = (
        df_kpi.groupby(["CD_CVM", "DT_INI_EXERC", "DT_FIM_EXERC"])
        .agg({"KPI": "max", "VL_CONTA": "sum", "EXERC_YEAR": "max"})
        .reset_index()
    )

    df_kpi["VL_CONTA_ROLLING_YEAR"] = -1
    df_kpi["VL_CONTA"] = df_kpi["VL_CONTA"] * 1000

    df_kpi = df_kpi.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

    print()
    print("DEBT")
    print(df_kpi.head(2))
    print()

    return df_kpi
