from utils import get_dre_kpi_info


def load_ebit(df_dre, df_reference_table):
    return get_dre_kpi_info("EBIT", df_dre, df_reference_table)
