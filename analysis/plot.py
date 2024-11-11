import pandas as pd
import matplotlib.pyplot as plt
import info
import measures
from sklearn import linear_model

_df_fundaments = pd.read_csv(
    "../data/processed/stocks-fundaments.csv",
    parse_dates=["DT_INI_EXERC", "DT_FIM_EXERC"],
)


def _plot_trend(x, y, label, color):
    # Giving more weight to data with dates closer to today
    days_diff = (x.max() - x.min()).days
    sample_weight = days_diff - (x.max() - x).dt.days

    x_train = x.values.astype(float).reshape(-1, 1)
    y_train = y.values.reshape(-1, 1)

    model = linear_model.LinearRegression()
    model.fit(x_train, y_train, sample_weight=sample_weight)

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

    if show_segment:
        kpi_info = measures.get_kpi_info(tickers[0], kpi, is_segemento=True)
        df_segmento, value_column, date_x_ticks = (
            kpi_info["df"],
            kpi_info["value_column"],
            kpi_info["date_x_ticks"],
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
        kpi_info = measures.get_kpi_info(ticker, kpi)
        df, value_column, date_x_ticks = (
            kpi_info["df"],
            kpi_info["value_column"],
            kpi_info["date_x_ticks"],
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
