import pandas as pd
import numpy as np


def get_ipca_weights(dates):
    df_ipca = pd.read_csv("../data/processed/ipca.csv", parse_dates=["DATE"])

    df_ipca["DATE"] = df_ipca["DATE"].dt.to_period("M")

    kpi_periods = dates.dt.to_period("M").values

    df_ipca = df_ipca[df_ipca["DATE"].isin(kpi_periods)]

    last_ipca_value = df_ipca["VALUE"].iloc[-1]

    weights = last_ipca_value / df_ipca["VALUE"]

    return weights.reset_index(drop=True).values


def get_date_weights(dates):
    days_diff = (dates.max() - dates.min()).days
    weights = (days_diff - (dates.max() - dates).dt.days) / days_diff

    return np.exp(weights.reset_index(drop=True).values)
