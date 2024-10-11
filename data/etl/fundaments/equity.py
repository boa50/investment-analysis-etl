from data.etl.utils import (
    get_kpi_fields,
)


def load_equity(df_bpp, df_reference_table, verbose=False):
    df_pl = get_kpi_fields(df_bpp, df_reference_table, "EQUITY")

    df_pl["VL_CONTA_ROLLING_YEAR"] = -1
    df_pl["VL_CONTA"] = df_pl["VL_CONTA"] * 1000

    df_pl = df_pl.sort_values(by=["DT_FIM_EXERC", "CD_CVM"])

    if verbose:
        print()
        print("PL")
        print(df_pl.head(2))
        print()

    return df_pl
