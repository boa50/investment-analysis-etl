import pandas as pd
import numpy as np
from sklearn import linear_model
import info
import utils

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


def _get_kpi_props(ticker, kpi, is_segemento=False):
    if kpi in _df_fundaments["KPI"].unique():
        return _get_kpi_props_fundaments(ticker, kpi, is_segemento=is_segemento)
    else:
        return _get_kpi_props_history(ticker, kpi, is_segemento=is_segemento)


def _get_drawdowns(
    risk_calculation_values, drawdown_kpi_multiplier=1, threshold=np.inf
):
    risk_values = np.minimum(
        risk_calculation_values * drawdown_kpi_multiplier,
        threshold * drawdown_kpi_multiplier,
    )
    running_max = np.maximum.accumulate(risk_values)
    drawdowns = ((risk_values - running_max) / running_max) * drawdown_kpi_multiplier

    return drawdowns.fillna(0)


def _get_risk_measures(drawdowns, weights=None):
    max_dd = np.min(drawdowns)
    pain_index = np.average(drawdowns, weights=weights)

    return {"max_dd": max_dd, "pain_index": pain_index}


def get_kpi_info(
    ticker,
    kpi,
    is_segmento=False,
    thresholds=[],
    is_time_weighted=False,
    is_inflation_weighted=False,
):
    print("Getting values for " + ticker)

    df, value_column, date_x_ticks = _get_kpi_props(
        ticker, kpi, is_segemento=is_segmento
    )

    weights = utils.get_date_weights(dates=df["DATE"]) if is_time_weighted else None

    if is_inflation_weighted and (kpi in ["PROFIT", "NET_REVENUE", "EBIT", "EQUITY"]):
        inflation_weights = utils.get_ipca_weights(dates=df["DATE"])
        if weights is None:
            weights = inflation_weights
        else:
            weights *= inflation_weights

    risk_calculation_values = df[value_column].copy()
    kpi_volatility = df[value_column].std()
    drawdowns = None

    if kpi in ["NET_DEBT_BY_EBIT", "NET_DEBT_BY_EQUITY"]:
        threshold = thresholds[0] if len(thresholds) == 1 else -np.inf
        drawdowns = _get_drawdowns(
            risk_calculation_values, drawdown_kpi_multiplier=-1, threshold=threshold
        )
    elif (kpi in ["DIVIDEND_PAYOUT"]) and (len(thresholds) > 1):
        drawdowns1 = _get_drawdowns(
            risk_calculation_values, drawdown_kpi_multiplier=1, threshold=thresholds[0]
        )
        drawdowns2 = _get_drawdowns(
            risk_calculation_values, drawdown_kpi_multiplier=-1, threshold=thresholds[1]
        )

        drawdowns = np.minimum(drawdowns1, drawdowns2)
    elif kpi in ["PL", "PVP"]:
        ticker_shape = risk_calculation_values.shape[0]

        df_segment, value_column_seg, _ = _get_kpi_props(ticker, kpi, is_segemento=True)
        trend_line = get_trend(
            df["DATE"],
            df_segment[value_column_seg][-ticker_shape:],
            is_time_weighted=False,
        )

        drawdowns = _get_drawdowns(
            risk_calculation_values + trend_line.reshape(-1),
            drawdown_kpi_multiplier=-1,
            threshold=-np.inf,
        )

    if drawdowns is None:
        drawdowns = _get_drawdowns(risk_calculation_values)

    risk_measures = _get_risk_measures(drawdowns, weights=weights)

    return {
        "dates": df["DATE"],
        "values": df[value_column] * (weights if weights is not None else 1),
        "x_ticks": df["DATE"][::date_x_ticks],
        "volatility": kpi_volatility,
        "max_drawdown": risk_measures["max_dd"],
        "pain_index": risk_measures["pain_index"],
    }


def get_trend(x, y, is_time_weighted=True):
    # Giving more weight to data with dates closer to today
    if is_time_weighted:
        sample_weight = utils.get_date_weights(dates=x)
    else:
        sample_weight = None

    x_train = x.values.astype(float).reshape(-1, 1)
    y_train = y.values.reshape(-1, 1)

    model = linear_model.LinearRegression()
    model.fit(x_train, y_train, sample_weight=sample_weight)

    trend = model.predict(x_train)

    print("m: " + str(model.coef_[0][0]))
    # print("Trend Last Value: " + str(trend[-1]))

    return trend


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
            "CAGR_5_YEARS_PROFIT",
            "CAGR_5_YEARS_REVENUE",
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
