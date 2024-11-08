import pandas as pd
import matplotlib.pyplot as plt
import info
from sklearn import linear_model

_df_history = pd.read_csv("../data/processed/stocks-history.csv", parse_dates=["DATE"])
_df_fundaments = pd.read_csv(
    "../data/processed/stocks-fundaments.csv",
    parse_dates=["DT_INI_EXERC", "DT_FIM_EXERC"],
)


def _get_chart_props_fundaments_segmento(ticker, df_kpi, value_column):
    segmento = info.get_segmento_by_ticker(ticker)
    cd_cvm_segmento = info.get_companies_by_segmento(segmento=segmento)["CD_CVM"].values

    df_segmento = df_kpi[df_kpi["CD_CVM"].isin(cd_cvm_segmento)]
    df_segmento = df_segmento[["DATE", value_column]]
    df_segmento = df_segmento.groupby("DATE").mean().reset_index()
    df_segmento = df_segmento.sort_values(by="DATE")

    return df_segmento


def _get_chart_props_fundaments(ticker, kpi, is_segemento=True):
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
        df = _get_chart_props_fundaments_segmento(ticker, df_kpi, value_column)
    else:
        df = df_kpi[df_kpi["CD_CVM"] == cd_cvm]

    date_x_ticks = 10

    return df, value_column, date_x_ticks


def _get_chart_props_history_segmento(ticker, kpi):
    segmento = info.get_segmento_by_ticker(ticker)
    tickers_segmento = info.get_companies_by_segmento(segmento=segmento)[
        "MAIN_TICKER"
    ].values
    df_segmento = _df_history[_df_history["TICKER"].isin(tickers_segmento)]
    df_segmento = df_segmento[["DATE", kpi]]
    df_segmento = df_segmento.groupby("DATE").mean().reset_index()
    df_segmento = df_segmento.sort_values(by="DATE")

    return df_segmento


def _get_chart_props_history(ticker, kpi, is_segemento=True):
    if is_segemento:
        df = _get_chart_props_history_segmento(ticker, kpi)
    else:
        df = _df_history[_df_history["TICKER"] == ticker]

    value_column = kpi
    date_x_ticks = 100

    return df, value_column, date_x_ticks


def _plot_trend(x, y, label, color):
    x_train = x.values.astype(float).reshape(-1, 1)
    y_train = y.values.reshape(-1, 1)

    model = linear_model.LinearRegression()
    model.fit(x_train, y_train)

    y_pred = model.predict(x_train)

    print(label)
    print("m: " + str(model.coef_[0][0]))
    print("Last Value: " + str(y_pred[-1]))

    plt.plot(
        x,
        y_pred,
        label=label,
        linestyle="--",
        linewidth=0.5,
        color=color,
    )


def plot_historical_kpi(tickers, kpi, show_segment=True, show_trend=False):
    plt.figure(figsize=(10, 5))
    is_df_fundaments = kpi in _df_fundaments["KPI"].unique()

    if show_segment:
        if is_df_fundaments:
            df_segmento, value_column, date_x_ticks = _get_chart_props_fundaments(
                tickers[0], kpi, is_segemento=True
            )
        else:
            df_segmento, value_column, date_x_ticks = _get_chart_props_history(
                tickers[0], kpi, is_segemento=True
            )

        segment_color = "grey"

        plt.plot(
            df_segmento["DATE"],
            df_segmento[value_column],
            label="Segment",
            linestyle="-.",
            color=segment_color,
        )

        if show_trend:
            _plot_trend(
                x=df_segmento["DATE"],
                y=df_segmento[value_column],
                label="Segment trend",
                color=segment_color,
            )

    for ticker in tickers:
        if is_df_fundaments:
            df, value_column, date_x_ticks = _get_chart_props_fundaments(
                ticker, kpi, is_segemento=False
            )
        else:
            df, value_column, date_x_ticks = _get_chart_props_history(
                ticker, kpi, is_segemento=False
            )

        p = plt.plot(
            df["DATE"],
            df[value_column],
            label=ticker,
            linestyle="-",
        )

        if show_trend:
            _plot_trend(
                x=df["DATE"],
                y=df[value_column],
                label=ticker + " trend",
                color=p[0].get_color(),
            )

    plt.title(kpi)
    plt.xlabel("Date")
    plt.ylabel(kpi)
    plt.xticks(df["DATE"][::date_x_ticks], rotation=0)
    plt.legend()

    plt.tight_layout()
    plt.show()


def plot_equity_evolution(ticker):
    cd_cvm = info.get_cd_cvm_by_ticker(ticker)
    first_date = _df_fundaments["DT_FIM_EXERC"].max() - pd.DateOffset(years=10)

    df_ticker = _df_fundaments[
        (_df_fundaments["CD_CVM"] == cd_cvm)
        & (_df_fundaments["DT_FIM_EXERC"] >= first_date)
    ]
    df_ticker = df_ticker.rename(columns={"DT_FIM_EXERC": "DATE"})

    df_net_revenue = df_ticker[df_ticker["KPI"] == "NET_REVENUE"]
    df_profit = df_ticker[df_ticker["KPI"] == "PROFIT"]
    df_equity = df_ticker[df_ticker["KPI"] == "EQUITY"]

    plt.figure(figsize=(10, 5))

    plt.plot(
        df_net_revenue["DATE"],
        df_net_revenue["VL_CONTA_ROLLING_YEAR"],
        label="Net Revenue",
        linestyle="-",
        linewidth=2.5,
        color="coral",
    )
    plt.plot(
        df_profit["DATE"],
        df_profit["VL_CONTA_ROLLING_YEAR"],
        label="Profit",
        linestyle="-",
        linewidth=2.5,
        color="limegreen",
    )
    plt.bar(
        df_equity["DATE"],
        df_equity["VL_CONTA"],
        label="Equity",
        width=70,
        color="steelblue",
    )

    plt.title("Equity Evolution - " + ticker)
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.xticks(df_net_revenue["DATE"][::10], rotation=0)
    plt.legend()

    plt.tight_layout()
    plt.show()
