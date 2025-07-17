import numpy as np
import pandas as pd
import data.etl.queries.queries as queries
import data.etl.mappings as mappings
import data.etl.ratings.utils as utils
import data.etl.ratings.calculations as calculations
from google.cloud import bigquery
from data.db_creation.schemas import get_schema
import os


def get_stock_ratings(
    ticker: str,
    df_segments: pd.DataFrame,
    df_fundaments: pd.DataFrame,
    df_history: pd.DataFrame,
    verbose: int = 0,
):
    def try_get_kpi_rating(ticker, kpi):
        kpi_rating = get_kpi_rating(
            ticker=ticker,
            kpi=kpi,
            df_segments=df_segments,
            df_fundaments=df_fundaments,
            df_history=df_history,
            verbose=verbose - 2,
        )

        if verbose > 1:
            print(kpi + ": " + str(kpi_rating))

        return kpi_rating

    ratings = {}
    for kpi_group in mappings.kpi_by_group.keys():
        ratings[kpi_group] = 0

    for kpi_group, kpis in mappings.kpi_by_group.items():
        for kpi in kpis:
            ratings[kpi_group] += try_get_kpi_rating(ticker=ticker, kpi=kpi)

        ratings[kpi_group] = ratings[kpi_group] * 100 / len(kpis)

    weights = [0.3, 0.3, 0.2, 0.1] if ratings["debt"] > 0 else [0.4, 0, 0.35, 0.25]

    ratings["overall"] = (np.array(list(ratings.values())) * np.array(weights)).sum()

    if verbose > 0:
        print()
        print(f"{ticker} RATINGS")
        for group, value in ratings.items():
            print(f"{group}: {value}")

    return ratings


def get_kpi_rating(
    ticker: str,
    kpi: str,
    df_segments: pd.DataFrame,
    df_fundaments: pd.DataFrame,
    df_history: pd.DataFrame,
    is_time_weighted: bool = True,
    is_inflation_weighted: bool = True,
    verbose: int = -1,
):
    df = get_kpi_values(
        ticker=ticker,
        kpi=kpi,
        df_segments=df_segments,
        df_fundaments=df_fundaments,
        df_history=df_history,
    )

    if (df.size == 0) or (np.sum(df["VALUE"] != 0) == 0):
        return 0

    df = df.dropna(axis=0)

    df_segment_ungrouped = get_kpi_values(
        ticker=ticker,
        kpi=kpi,
        df_segments=df_segments,
        df_fundaments=df_fundaments,
        df_history=df_history,
        is_from_segment=True,
        group_segment_values=False,
    )

    df_segment = get_kpi_values(
        ticker=ticker,
        kpi=kpi,
        df_segments=df_segments,
        df_fundaments=df_fundaments,
        df_history=df_history,
        is_from_segment=True,
        group_segment_values=True,
    )

    weights = utils.get_date_weights(dates=df["DATE"]) if is_time_weighted else None

    if is_inflation_weighted and (kpi in ["PROFIT", "NET_REVENUE", "EBIT", "EQUITY"]):
        inflation_weights = utils.get_ipca_weights(dates=df["DATE"])
        if weights is None:
            weights = inflation_weights
        else:
            weights *= inflation_weights

    if kpi in ["NET_DEBT_BY_EBIT", "NET_DEBT_BY_EQUITY"]:
        thresholds = [3]
    elif kpi in ["DIVIDEND_PAYOUT"]:
        thresholds = [20, 70]
    else:
        thresholds = []

    pain_index = calculations.calculate_kpi_pain_index(
        df=df,
        kpi=kpi,
        df_segment=df_segment,
        thresholds=thresholds,
        weights=weights,
    )

    _, slope = calculations.calculate_trend(
        x=df["DATE"], y=df["VALUE"], sample_weight=weights
    )

    last_value = float(df.iat[-1, -1])

    if verbose > 0:
        print()
        print("get_kpi_rating")
        print(f"pain_index: {pain_index}")
        print(f"slope: {slope}")
        print(f"last_value: {last_value}")

    rating = calculations.calculate_kpi_rating(
        pain_index=pain_index,
        slope=slope,
        last_value=last_value,
        df_segment_ungrouped=df_segment_ungrouped,
        kpi=kpi,
        verbose=verbose - 1,
    )

    return rating


def get_kpi_values(
    ticker: str,
    kpi: str,
    df_segments: pd.DataFrame,
    df_fundaments: pd.DataFrame,
    df_history: pd.DataFrame,
    is_from_segment: bool = False,
    group_segment_values: bool = True,
):
    table_origin = mappings.kpi_table_origin[kpi]

    if table_origin == "fundaments":
        value_column = mappings.kpi_fundament_value_column[kpi]

        if is_from_segment:
            cds_cvm = get_segment_cds_cvm_by_ticker(df_segments, ticker)
        else:
            cds_cvm = df_segments[df_segments["TICKER"] == ticker]["CD_CVM"].values

        df = df_fundaments.copy()
        df = df[df["CD_CVM"].isin(cds_cvm)]
        df = df[df["KPI"] == kpi]
        df["VALUE_NEW"] = df[value_column]
        df = df.drop(["VALUE", "VALUE_ROLLING_YEAR"], axis=1)
        df = df.rename(columns={"VALUE_NEW": "VALUE"})

        if group_segment_values:
            df = df.drop("CD_CVM", axis=1)
            df = df.groupby(["KPI", "DATE"]).mean().reset_index()
            df = df.sort_values(by=["DATE"])
        else:
            df = df.sort_values(by=["CD_CVM", "DATE"])

        return df
    elif table_origin == "history":
        if is_from_segment:
            tickers = get_segment_tickers_by_ticker(df_segments, ticker)
        else:
            tickers = [ticker]

        df = df_history.copy()
        df = df[df["TICKER"].isin(tickers)]
        df = df[["DATE", "CD_CVM", "TICKER", kpi]]
        df = df.rename(columns={kpi: "VALUE"})

        if group_segment_values:
            df = df.drop(["CD_CVM", "TICKER"], axis=1)
            df = df.groupby(["DATE"]).mean().reset_index()
            df = df.sort_values(by=["DATE"])
        else:
            df = df.sort_values(by=["TICKER", "DATE"])

        return df
    else:
        return pd.DataFrame()


def get_segment_cds_cvm_by_ticker(df_segments, ticker):
    segment = df_segments[df_segments["TICKER"] == ticker]["SEGMENT"].values[0]
    cds_cvm = df_segments[df_segments["SEGMENT"] == segment]["CD_CVM"]

    return cds_cvm.drop_duplicates().values


def get_segment_tickers_by_ticker(df_segments, ticker):
    segment = df_segments[df_segments["TICKER"] == ticker]["SEGMENT"].values[0]
    tickers = df_segments[df_segments["SEGMENT"] == segment]["TICKER"]

    return tickers.values


def get_ratings(verbose: int = 0):
    df_segments = queries.get_segments()
    df_fundaments = queries.get_fundaments(n_years=10)
    df_history = queries.get_history(n_years=10)

    tickers = df_history["TICKER"].unique()
    all_ratings = []

    for ticker in tickers:
        if verbose > 0:
            print()
            print(f"Getting ratings for {ticker}")

        ratings = {"ticker": ticker}

        ratings_tmp = get_stock_ratings(
            ticker=ticker,
            df_segments=df_segments,
            df_fundaments=df_fundaments,
            df_history=df_history,
            verbose=verbose - 1,
        )

        ratings = {**ratings, **ratings_tmp}

        all_ratings.append(ratings)

    return all_ratings


def load_ratings_to_db():
    ratings = get_ratings(verbose=1)

    client = bigquery.Client()
    dataset_id = f"{os.environ.get('DB_PROJECT_ID')}.{os.environ.get('DB_DATASET_ID')}"
    table_name = "stocks-ratings"

    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=get_schema(table_name=table_name),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(ratings, table_id, job_config=job_config)

    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


load_ratings_to_db()
