import math
import numpy as np
import pandas as pd
from sklearn import linear_model


def calculate_kpi_rating(
    pain_index: float,
    slope: float,
    last_value: float,
    df_segment_ungrouped: pd.DataFrame,
    kpi: str,
    kpi_cap_jitter: float = 0.05,
    slope_cap: float = 5,
    pain_index_weight: float = 0.1,
    slope_weight: float = 0.25,
    last_value_weight: float = 0.65,
    verbose: int = -1,
):
    pain_index_normalised = max(pain_index, -1) + 1

    slope *= math.pow(10, get_number_length(slope))

    group_column = "TICKER" if "TICKER" in df_segment_ungrouped.columns else "CD_CVM"

    df_segment_last_dates = (
        df_segment_ungrouped.groupby(group_column)["DATE"].max().reset_index()
    )

    df_segment_last_dates = df_segment_last_dates.merge(
        df_segment_ungrouped, on=[group_column, "DATE"]
    )
    kpi_values = df_segment_last_dates["VALUE"].values.astype(float)

    kpi_min = kpi_values.min()
    kpi_min = (
        kpi_min * (1 + kpi_cap_jitter)
        if kpi_min < 0
        else kpi_min * (1 - kpi_cap_jitter)
    )
    kpi_max = kpi_values.max()
    kpi_max = (
        kpi_max * (1 + kpi_cap_jitter)
        if kpi_max > 0
        else kpi_max * (1 - kpi_cap_jitter)
    )

    last_value_normalised = (last_value - kpi_min) / (kpi_max - kpi_min)

    slope_min = -slope_cap
    slope_capped = max(min(slope_cap, slope), slope_min)
    slope_normalised = (slope_capped - slope_min) / (slope_cap - slope_min)

    if kpi in [
        "PL",
        "PVP",
        "NET_DEBT_BY_EBIT",
        "NET_DEBT_BY_EQUITY",
        "DEBT",
        "DEBT_NET",
    ]:
        last_value_normalised = (last_value_normalised - 1) * -1
        slope_normalised = (slope_normalised - 1) * -1

    rating = (
        pain_index_normalised * pain_index_weight
        + slope_normalised * slope_weight
        + last_value_normalised * last_value_weight
    )

    if verbose > 0:
        print()
        print("calculate_kpi_rating")
        print(f"pain_index_normalised: {pain_index_normalised}")
        print(f"slope_normalised: {slope_normalised}")
        print(f"last_value_normalised: {last_value_normalised}")

    return rating


def calculate_kpi_pain_index(
    df: pd.DataFrame,
    kpi: str,
    df_segment: pd.DataFrame,
    thresholds: list = [],
    weights: list = None,
):
    values = df["VALUE"]
    drawdowns = None

    if kpi in ["NET_DEBT_BY_EBIT", "NET_DEBT_BY_EQUITY"]:
        threshold = thresholds[0] if len(thresholds) == 1 else -np.inf
        drawdowns = calculate_drawdowns(
            values, drawdown_kpi_multiplier=-1, threshold=threshold
        )
    elif (kpi in ["DIVIDEND_PAYOUT"]) and (len(thresholds) > 1):
        drawdowns1 = calculate_drawdowns(
            values, drawdown_kpi_multiplier=1, threshold=thresholds[0]
        )
        drawdowns2 = calculate_drawdowns(
            values, drawdown_kpi_multiplier=-1, threshold=thresholds[1]
        )

        drawdowns = np.minimum(drawdowns1, drawdowns2)
    elif kpi in ["PL", "PVP"]:
        ticker_shape = values.shape[0]

        trend_line, _ = calculate_trend(df["DATE"], df_segment["VALUE"][-ticker_shape:])

        # Filling undesired negative values to avoid problems with the drawdowns calculation
        values = values.where(values >= 0).bfill()

        drawdowns = calculate_drawdowns(
            values + trend_line.reshape(-1),
            drawdown_kpi_multiplier=-1,
            threshold=-np.inf,
        )

    if drawdowns is None:
        drawdowns = calculate_drawdowns(values)

    pain_index = calculate_pain_index(drawdowns, weights=weights)

    return pain_index


def calculate_trend(x, y, sample_weight=None):
    x_train = pd.to_datetime(x).values.astype(float).reshape(-1, 1)
    y_train = y.values.reshape(-1, 1)

    model = linear_model.LinearRegression()
    model.fit(x_train, y_train, sample_weight=sample_weight)

    trend = model.predict(x_train)
    m = model.coef_[0][0]

    return trend, m


def calculate_drawdowns(
    risk_calculation_values: pd.Series,
    drawdown_kpi_multiplier: float = 1,
    threshold: float = np.inf,
):
    risk_values = np.minimum(
        risk_calculation_values * drawdown_kpi_multiplier,
        threshold * drawdown_kpi_multiplier,
    ).astype(float)
    running_max = np.maximum.accumulate(risk_values).astype(float)

    drawdowns = ((risk_values - running_max) / running_max) * drawdown_kpi_multiplier

    return drawdowns.fillna(0)


def calculate_pain_index(drawdowns, weights=None):
    pain_index = np.average(drawdowns, weights=weights)

    return pain_index


def get_number_length(number):
    length = abs(int(math.log10(abs(number))))

    if number < 0:
        length += 1

    return length
