import matplotlib.pyplot as plt
import measures


def _plot_trend(x, y, label, color):
    trend = measures.get_trend(x, y)

    plt.plot(
        x,
        trend,
        label=label,
        linestyle="--",
        linewidth=0.5,
        color=color,
    )


def plot_historical_kpi(
    tickers, kpi, show_segment=True, show_trend=False, is_inflation_weighted=False
):
    plt.figure(figsize=(10, 5))

    if show_segment:
        kpi_info = measures.get_kpi_info(
            tickers[0],
            kpi,
            is_segemento=True,
            is_inflation_weighted=is_inflation_weighted,
        )
        dates, values, x_ticks = (
            kpi_info["dates"],
            kpi_info["values"],
            kpi_info["x_ticks"],
        )

        segment_color = "grey"

        plt.plot(
            dates,
            values,
            label="Segment",
            linestyle="-.",
            color=segment_color,
        )

        if show_trend:
            _plot_trend(
                x=dates,
                y=values,
                label="Segment trend",
                color=segment_color,
            )

    for ticker in tickers:
        kpi_info = measures.get_kpi_info(
            ticker, kpi, is_inflation_weighted=is_inflation_weighted
        )
        dates, values, x_ticks = (
            kpi_info["dates"],
            kpi_info["values"],
            kpi_info["x_ticks"],
        )

        p = plt.plot(
            dates,
            values,
            label=ticker,
            linestyle="-",
        )

        if show_trend:
            _plot_trend(
                x=dates,
                y=values,
                label=ticker + " trend",
                color=p[0].get_color(),
            )

    plt.title(kpi)
    plt.xlabel("Date")
    plt.ylabel(kpi)
    plt.xticks(x_ticks, rotation=0)
    plt.legend()

    plt.tight_layout()
    plt.show()


def plot_equity_evolution(ticker, is_inflation_weighted=False):
    def get_kpi_data(kpi):
        kpi_info = measures.get_kpi_info(
            ticker, kpi, is_inflation_weighted=is_inflation_weighted
        )

        return kpi_info["dates"], kpi_info["values"], kpi_info["x_ticks"]

    net_revenue_dates, net_revenue_values, x_ticks = get_kpi_data("NET_REVENUE")
    profit_dates, profit_values, _ = get_kpi_data("PROFIT")
    equity_dates, equity_values, _ = get_kpi_data("EQUITY")

    plt.figure(figsize=(10, 5))

    plt.plot(
        net_revenue_dates,
        net_revenue_values,
        label="Net Revenue",
        linestyle="-",
        linewidth=2.5,
        color="coral",
    )
    plt.plot(
        profit_dates,
        profit_values,
        label="Profit",
        linestyle="-",
        linewidth=2.5,
        color="limegreen",
    )
    plt.bar(
        equity_dates,
        equity_values,
        label="Equity",
        width=70,
        color="steelblue",
    )

    plt.title("Equity Evolution - " + ticker)
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.xticks(x_ticks, rotation=0)
    plt.legend()

    plt.tight_layout()
    plt.show()
