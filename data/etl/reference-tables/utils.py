import pandas as pd


def clear_table(df, cd_cvm_load):
    df_fields = df.copy()
    df_fields = df_fields[df_fields["CD_CVM"].isin(cd_cvm_load)]
    df_fields["DS_CONTA"] = df_fields["DS_CONTA"].str.lower()

    df_stocks_files = pd.read_csv("data/processed/stocks-files.csv")

    df_fields = df_fields.merge(
        df_stocks_files, how="inner", on=["CD_CVM", "FILE_CATEGORY"]
    )

    df_fields = df_fields[
        ["CD_CVM", "FILE_CATEGORY", "FILE_CATEGORY_SHORT", "CD_CONTA", "DS_CONTA"]
    ].drop_duplicates()

    return df_fields


def get_years_load():
    year_initial = 2011
    year_final = 2024
    years_load = list(range(year_initial, year_final + 1))

    return years_load


def get_cd_cvm_load(kpi):
    ### Getting only companies available on basic info file that are not in the reference table file
    df_reference_table = pd.read_csv("data/processed/reference-table.csv")
    df_reference_table = df_reference_table[df_reference_table["KPI"] == kpi]

    df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

    # Banks don't have EBITDA and DEBT
    # https://investalk.bb.com.br/noticias/quero-aprender/entenda-o-balanco-dos-bancos-e-porque-e-diferente-de-outras-empresas
    kpis_banks_dont_have = ["EBITDA-NEG", "DEBT", "DEBT-NEG"]
    if kpi in kpis_banks_dont_have:
        df_basic_info = df_basic_info[df_basic_info["SEGMENTO"] != "Bancos"]

    cd_cvm_load = list(
        set(df_basic_info["CD_CVM"].values).difference(
            df_reference_table["CD_CVM"].values
        )
    )

    return cd_cvm_load
