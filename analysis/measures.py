import pandas as pd
import info

_df_basic_info = pd.read_csv("../data/processed/stocks-basic-info.csv")
_df_history = pd.read_csv("../data/processed/stocks-history.csv", parse_dates=["DATE"])
_df_fundaments = pd.read_csv(
    "../data/processed/stocks-fundaments.csv",
    parse_dates=["DT_INI_EXERC", "DT_FIM_EXERC"],
)
_df_right_prices = pd.read_csv("../data/processed/stocks-right-prices.csv")


def _get_kpi_props_fundaments_segmento(ticker, df_kpi, value_column):
    segmento = info.get_segmento_by_ticker(ticker)
    cd_cvm_segmento = info.get_companies_by_segmento(segmento=segmento)["CD_CVM"].values

    df_segmento = df_kpi[df_kpi["CD_CVM"].isin(cd_cvm_segmento)]
    df_segmento = df_segmento[["DATE", value_column]]
    df_segmento = df_segmento.groupby("DATE").mean().reset_index()
    df_segmento = df_segmento.sort_values(by="DATE")

    return df_segmento


def _get_kpi_props_fundaments(ticker, kpi, is_segemento=True):
    cd_cvm = info.get_cd_cvm_by_ticker(ticker)
    first_date = _df_fundaments["DT_FIM_EXERC"].max() - pd.DateOffset(years=10)

    df_kpi = _df_fundaments[
        (_df_fundaments["KPI"] == kpi) & (_df_fundaments["DT_FIM_EXERC"] >= first_date)
    ]

    df_kpi = df_kpi.rename(columns={"DT_FIM_EXERC": "DATE"})

    value_column = (
        "VL_CONTA"
        if df_kpi["VL_CONTA_ROLLING_YEAR"].max() == -1
        else "VL_CONTA_ROLLING_YEAR"
    )

    if is_segemento:
        df = _get_kpi_props_fundaments_segmento(ticker, df_kpi, value_column)
    else:
        df = df_kpi[df_kpi["CD_CVM"] == cd_cvm]

    date_x_ticks = 10

    return df, value_column, date_x_ticks


def _get_kpi_props_history_segmento(ticker, kpi):
    segmento = info.get_segmento_by_ticker(ticker)
    tickers_segmento = info.get_companies_by_segmento(segmento=segmento)[
        "MAIN_TICKER"
    ].values
    df_segmento = _df_history[_df_history["TICKER"].isin(tickers_segmento)]
    df_segmento = df_segmento[["DATE", kpi]]
    df_segmento = df_segmento.groupby("DATE").mean().reset_index()
    df_segmento = df_segmento.sort_values(by="DATE")

    return df_segmento


def _get_kpi_props_history(ticker, kpi, is_segemento=True):
    if is_segemento:
        df = _get_kpi_props_history_segmento(ticker, kpi)
    else:
        df = _df_history[_df_history["TICKER"] == ticker]

    value_column = kpi
    date_x_ticks = 100

    return df, value_column, date_x_ticks


def get_kpi_info(ticker, kpi, is_segmento=False):
    if kpi in _df_fundaments["KPI"].unique():
        df, value_column, date_x_ticks = _get_kpi_props_fundaments(
            ticker, kpi, is_segemento=is_segmento
        )
    else:
        df, value_column, date_x_ticks = _get_kpi_props_history(
            ticker, kpi, is_segemento=is_segmento
        )

    kpi_variance = df[value_column].var()

    return {
        "df": df,
        "value_column": value_column,
        "date_x_ticks": date_x_ticks,
        "variance": kpi_variance,
    }


def get_latest_values_by_ticker(ticker):
    df_history_tmp = _df_history[_df_history["TICKER"] == ticker]
    cd_cvm = df_history_tmp.iloc[0]["CD_CVM"]

    df_fundaments_tmp = _df_fundaments[_df_fundaments["CD_CVM"] == cd_cvm]
    last_dt_fim_exerc = df_fundaments_tmp["DT_FIM_EXERC"].max()

    df_fundaments_tmp = df_fundaments_tmp[
        df_fundaments_tmp["DT_FIM_EXERC"] == last_dt_fim_exerc
    ]
    df_fundaments_tmp_2 = df_fundaments_tmp[
        df_fundaments_tmp["VL_CONTA_ROLLING_YEAR"] == -1
    ]
    df_fundaments_tmp = df_fundaments_tmp[
        df_fundaments_tmp["VL_CONTA_ROLLING_YEAR"] != -1
    ]
    df_fundaments_tmp = df_fundaments_tmp.pivot(
        index="DT_FIM_EXERC", columns="KPI", values="VL_CONTA_ROLLING_YEAR"
    ).reset_index()
    df_fundaments_tmp_2 = df_fundaments_tmp_2.pivot(
        index="DT_FIM_EXERC", columns="KPI", values="VL_CONTA"
    ).reset_index()

    df_fundaments_tmp = pd.concat([df_fundaments_tmp, df_fundaments_tmp_2], axis=1)
    df_fundaments_tmp = df_fundaments_tmp.drop("DT_FIM_EXERC", axis=1)

    df_history_tmp = df_history_tmp.tail(1).reset_index(drop=True)

    df_right_prices_tmp = _df_right_prices[_df_right_prices["TICKER"] == ticker]
    df_right_prices_tmp = df_right_prices_tmp.drop(
        ["CD_CVM", "TICKER"], axis=1
    ).reset_index(drop=True)

    df_latest_values = pd.concat(
        [df_history_tmp, df_right_prices_tmp, df_fundaments_tmp], axis=1
    )

    total_stocks = _df_basic_info[_df_basic_info["CD_CVM"] == cd_cvm].iloc[0][
        "NUM_TOTAL"
    ]

    df_latest_values["MARKET_CAP"] = df_latest_values["PRICE"] * total_stocks

    return df_latest_values


def get_latest_values_by_segmento(segmento):
    df_segment = pd.DataFrame()

    for ticker in info.get_companies_by_segmento(segmento=segmento)[
        "MAIN_TICKER"
    ].values:
        df_segment = pd.concat([df_segment, get_latest_values_by_ticker(ticker=ticker)])

    df_segment = df_segment.sort_values(by="MARKET_CAP", ascending=False)

    df_segment = df_segment.reset_index(drop=True)
    df_segment_tmp = df_segment.drop(
        ["DATE", "CD_CVM", "TICKER", "BAZIN", "PRICE"], axis=1
    )

    market_cap_kpis = ["EBIT", "PROFIT", "NET_REVENUE", "EQUITY", "MARKET_CAP"]

    df_segment_row = df_segment_tmp.drop(market_cap_kpis, axis=1).mean().to_frame().T
    df_segment_row["TICKER"] = "SEGMENTO"

    df_segment_cols = df_segment_tmp[market_cap_kpis].sum().to_frame().T
    df_segment_cols.columns = [col + "_SEGMENTO" for col in df_segment_cols.columns]

    df_final = pd.concat([df_segment, df_segment_cols], axis=1).ffill()

    for col in df_final.columns:
        if "_SEGMENTO" in col:
            df_final[col] = (
                df_final[col] * df_final["MARKET_CAP"] / df_final["MARKET_CAP_SEGMENTO"]
            )

    df_final = df_final.drop(["DATE", "CD_CVM", "MARKET_CAP_SEGMENTO"], axis=1)
    df_final = pd.concat([df_final, df_segment_row])
    df_final = df_final.fillna(0)
    return df_final[
        [
            "TICKER",
            "PRICE",
            "BAZIN",
            "PL",
            "PVP",
            "DIVIDEND_YIELD",
            "DIVIDEND_PAYOUT",
            "ROE",
            "CGAR_5_YEARS_PROFIT",
            "CGAR_5_YEARS_REVENUE",
            "NET_MARGIN",
            "NET_DEBT_BY_EBIT",
            "NET_DEBT_BY_EQUITY",
            "PROFIT",
            "PROFIT_SEGMENTO",
            "NET_REVENUE",
            "NET_REVENUE_SEGMENTO",
            "EBIT",
            "EBIT_SEGMENTO",
            # "EQUITY",
            # "EQUITY_SEGMENTO",
            # "MARKET_CAP",
        ]
    ]
