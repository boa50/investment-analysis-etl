from utils import get_dre_kpi_info, get_cagr


def load_net_revenue(df_dre, df_reference_table, verbose=False):
    return get_dre_kpi_info("NET_REVENUE", df_dre, df_reference_table, verbose=verbose)


def load_cagr_revenue_5_years(df_net_revenue, verbose=False):
    df_kpi = get_cagr(df_net_revenue)
    df_kpi["KPI"] = "CAGR_5_YEARS_REVENUE"

    if verbose:
        print()
        print("CAGR REVENUE 5 YEARS")
        print(df_kpi.tail())
        print()

    return df_kpi
