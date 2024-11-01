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
