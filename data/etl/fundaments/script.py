import pandas as pd
import numpy as np
from data.etl.utils import (
    load_files,
    prepare_dataframe,
)
from data.etl.fundaments.profit import (
    load_profit,
    load_cagr_profit_5_years,
    load_net_margin,
)
from data.etl.fundaments.earnings import load_ebit
from data.etl.fundaments.equity import load_equity
from data.etl.fundaments.roe import load_roe
from data.etl.fundaments.debt import (
    load_total_debt,
    load_net_debt,
    load_net_debt_by_ebit,
    load_net_debt_by_equity,
)
from data.etl.fundaments.revenue import load_net_revenue, load_cagr_revenue_5_years
import data.etl.queries.queries as qu
import data.db_creation.batch_load as batch_load
from pathlib import Path

from os import listdir
from os.path import isfile, join


def process_files_to_csv():
    data_files = [f for f in listdir("data/raw") if isfile(join("data/raw", f))]
    data_files = [f for f in data_files if "itr_cia_aberta_" in f]
    years_load = list(set([int(f[-8:-4]) for f in data_files]))

    df_fundaments = pd.DataFrame(
        columns=[
            "CD_CVM",
            "DT_INI_EXERC",
            "DT_FIM_EXERC",
            "KPI",
            "VL_CONTA",
            "EXERC_YEAR",
            "VL_CONTA_ROLLING_YEAR",
        ]
    )

    cd_cvm_load = [int(cd_cvm) for cd_cvm in qu.get_available_stocks()["CD_CVM"].values]

    ### Getting the KPIs reference table
    df_reference_table = pd.read_csv("data/processed/reference-table.csv")

    ### KPIs from DRE files
    df_dre = load_files(years_load, files_types_load=["DRE"])
    df_dre = prepare_dataframe(df_dre, cd_cvm_load)

    df_profit = load_profit(df_dre, df_reference_table)
    df_ebit = load_ebit(df_dre, df_reference_table)
    df_net_revenue = load_net_revenue(df_dre, df_reference_table)

    # Removed the load of the ebitda because each company show the results differently
    # and not all of them have values in the DRE files
    # A better alternative would be to scrap data directly from the RIs
    # df_ebitda = load_ebitda(df_dre, df_ebit, df_reference_table)

    del df_dre

    ### KPIs from BPP files
    df_bpp = load_files(years_load, files_types_load=["BPP"])
    df_bpp["DT_INI_EXERC"] = "1900-01-01"
    df_bpp = prepare_dataframe(df_bpp, cd_cvm_load)

    df_equity = load_equity(df_bpp, df_reference_table)
    df_total_debt = load_total_debt(df_bpp, df_reference_table)

    del df_bpp

    ## KPIs from BPA files
    df_bpa = load_files(years_load, files_types_load=["BPA"])
    df_bpa["DT_INI_EXERC"] = "1900-01-01"
    df_bpa = prepare_dataframe(df_bpa, cd_cvm_load)

    df_net_debt = load_net_debt(df_bpa, df_total_debt, df_reference_table)

    del df_bpa

    ### Consolidate the final file
    df_roe = load_roe(df_profit, df_equity)
    # df_net_debt_by_ebitda = load_net_debt_by_ebitda(df_net_debt, df_ebitda)
    df_net_debt_by_ebit = load_net_debt_by_ebit(df_net_debt, df_ebit)
    df_net_debt_by_equity = load_net_debt_by_equity(df_net_debt, df_equity)
    df_cagr_profit_5_years = load_cagr_profit_5_years(df_profit)
    df_cagr_revenue_5_years = load_cagr_revenue_5_years(df_net_revenue)
    df_net_margin = load_net_margin(df_profit, df_net_revenue)

    df_fundaments = pd.concat(
        [
            df_fundaments,
            df_profit,
            df_equity,
            df_roe,
            df_ebit,
            # df_ebitda,
            df_total_debt,
            df_net_debt,
            # df_net_debt_by_ebitda,
            df_net_debt_by_ebit,
            df_cagr_profit_5_years,
            df_net_revenue,
            df_cagr_revenue_5_years,
            df_net_margin,
            df_net_debt_by_equity,
        ]
    )

    df_fundaments = df_fundaments.sort_values(
        by=[
            "KPI",
            "CD_CVM",
            "DT_FIM_EXERC",
        ]
    )

    df_fundaments = df_fundaments.dropna(subset=["VL_CONTA"])

    ### The code below is to fix some unknown bug with the dates
    df_fundaments["DT_INI_EXERC"] = pd.to_datetime(
        df_fundaments["DT_INI_EXERC"], format="mixed"
    ).dt.date
    df_fundaments["DT_FIM_EXERC"] = pd.to_datetime(
        df_fundaments["DT_FIM_EXERC"], format="mixed"
    ).dt.date

    print()
    print("DF FUNDAMENTS")
    print(df_fundaments.head())
    print(df_fundaments.tail())
    print()

    df_fundaments.to_csv("data/processed/stocks-fundaments.csv", index=False)


def update_database(years_to_update: list):
    csv_path = "data/processed/stocks-fundaments.csv"
    df = pd.read_csv(csv_path)
    df = df[df["EXERC_YEAR"].isin(years_to_update)]

    qu.delete_data_from_fundaments(years_to_delete=years_to_update)

    df = df.replace([np.inf, -np.inf], np.nan)
    df.columns = [
        "CD_CVM",
        "DT_START",
        "DT_END",
        "KPI",
        "VALUE",
        "DT_YEAR",
        "VALUE_ROLLING_YEAR",
    ]
    df = df[
        [
            "CD_CVM",
            "DT_START",
            "DT_END",
            "DT_YEAR",
            "KPI",
            "VALUE",
            "VALUE_ROLLING_YEAR",
        ]
    ]

    batch_load.load_data(table_name="stocks-fundaments", df=df)

    Path(csv_path).unlink(missing_ok=True)
