from data.etl.utils import (
    get_kpi_fields,
    transform_anual_quarter,
)


def get_dre_kpi_info(kpi, df_dre, df_reference_table):
    df_kpi = get_kpi_fields(df_dre, df_reference_table, kpi)
    df_kpi = transform_anual_quarter(df_kpi)

    print()
    print(kpi)
    print(df_kpi.tail(2))
    print()

    return df_kpi
