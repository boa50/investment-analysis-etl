import pymupdf
import pandas as pd
import numpy as np
import os
import queries
from multiprocessing import Pool, cpu_count


def _get_doc_dividends(doc_path: str):
    doc = pymupdf.open(doc_path)

    for page in doc:
        tabs = page.find_tables()
        if tabs.tables:
            df_version = pd.DataFrame(tabs[0].extract())

            df = pd.DataFrame(tabs[1].extract())
            is_installments = df.iat[2, 5] == "Parcelado"

            if not is_installments:
                df = df.iloc[2:]
                df = df[[0, 1, 6]]
                df.columns = ["ISIN", "VALUE", "DATE"]

                df["DATE"] = df["DATE"].replace("", np.nan)
                df = df.dropna()
            else:
                df = df.iloc[2:]
                df = df[[0]]
                df.columns = ["ISIN"]

                df_installments = pd.DataFrame(tabs[2].extract())
                df_installments = df_installments.iloc[1:]
                df_installments.columns = ["DATE", "VALUE"]

                df_installments["DATE"] = df_installments["DATE"].replace("", np.nan)
                df_installments = df_installments.dropna()

                df_installments_length = df_installments.shape[0]
                if df_installments_length == 0:
                    return pd.DataFrame()

                df_installments = df_installments.sort_values(by="DATE")

                df_installments["ISIN"] = np.resize(
                    df["ISIN"].values, df_installments_length
                )

                df = df_installments[["ISIN", "VALUE", "DATE"]].copy()

            df["DOC_DATE"] = df_version.iat[5, 0]
            df["DOC_VERSION"] = df_version.iat[7, 1]

            return df


def _clean_df_dividends(df: pd.DataFrame):
    new_df = df.copy()

    new_df = new_df.replace("\n", "", regex=True)

    df_code_ticker = pd.read_csv(
        "data/raw/b3_stocks_codes_tickers.csv",
        encoding="ISO-8859-1",
        skiprows=1,
        sep=";",
        usecols=["TckrSymb", "ISIN"],
    )

    df_code_ticker.columns = ["TICKER", "ISIN"]
    df_code_ticker = df_code_ticker[
        df_code_ticker["TICKER"].str.len().isin([5, 6])
        & ~df_code_ticker["TICKER"].str[-1].isin(["F", "M", "Q", "R"])
    ]

    new_df = pd.merge(new_df, df_code_ticker, on="ISIN", how="left")
    new_df = new_df.dropna(subset="TICKER", axis=0)

    new_df["VALUE"] = new_df["VALUE"].str.replace(",", ".").astype(float)
    new_df["DATE"] = pd.to_datetime(new_df["DATE"], dayfirst=True)
    new_df["DOC_DATE"] = pd.to_datetime(
        new_df["DOC_DATE"], format="mixed", dayfirst=True
    )

    docs_groups = (
        new_df[["DOC_DATE", "DOC_VERSION"]].groupby("DOC_DATE").max().reset_index()
    )

    new_df = new_df.merge(docs_groups, how="inner", on=["DOC_DATE", "DOC_VERSION"])

    new_df = (
        new_df[["TICKER", "DATE", "VALUE", "DOC_DATE", "DOC_VERSION"]]
        .sort_values(by=["TICKER", "DATE"])
        .reset_index(drop=True)
    )

    return new_df


def _calculate_value_splits(df: pd.DataFrame):
    df_all_dividends = pd.DataFrame()
    tickers = df["TICKER"].unique()
    stocks_splits = queries.get_stocks_splits()
    stocks_splits["DATE"] = pd.to_datetime(stocks_splits["DATE"])
    stocks_splits["PROPORTION"] = stocks_splits["PROPORTION"].astype(float)

    for ticker in tickers:
        cd_cvm = queries.get_cd_cvm_by_ticker(ticker=ticker)
        stocks_splits_tk = stocks_splits[stocks_splits["CD_CVM"] == cd_cvm]
        df_dividends_tk = df[df["TICKER"] == ticker]

        for _, row in stocks_splits_tk.iterrows():
            df_dividends_tk.loc[df_dividends_tk["DATE"] <= row["DATE"], "VALUE"] /= row[
                "PROPORTION"
            ]

        df_all_dividends = pd.concat([df_all_dividends, df_dividends_tk])

    return df_all_dividends


def process_docs_parallel(doc_path: str, doc: str):
    df_doc_dividends = _get_doc_dividends(doc_path=doc_path)

    df_docs_processed = pd.DataFrame(data=[doc], columns=["FILE_NAME"])

    if (df_doc_dividends is not None) and (df_doc_dividends.shape[0] > 0):
        return (df_doc_dividends, df_docs_processed)
    else:
        return (pd.DataFrame(), df_docs_processed)


def load_dividends_from_pdf():
    df_all_dividends = pd.DataFrame()
    df_docs_processed = pd.DataFrame()

    docs_path = "data/raw/dividends/"
    docs = os.listdir(docs_path)

    pool = Pool(processes=(cpu_count() - 1))

    processes_results = []
    ref_df_all_dividends = []
    ref_df_docs_processed = []
    for doc in docs:
        doc_path = docs_path + doc

        result = pool.apply_async(process_docs_parallel, args=(doc_path, doc))

        result_dfs = result.get()
        ref_df_all_dividends.append(result_dfs[0])
        ref_df_docs_processed.append(result_dfs[1])

        processes_results.append(result)

    [result.wait() for result in processes_results]

    pool.close()
    pool.join()

    df_all_dividends = pd.concat(ref_df_all_dividends)
    df_docs_processed = pd.concat(ref_df_docs_processed)

    df_all_dividends = _clean_df_dividends(df_all_dividends)

    df_all_dividends = _calculate_value_splits(df=df_all_dividends)

    df_all_dividends.to_csv("data/processed/stocks-dividends.csv", index=False)
    df_docs_processed.to_csv(
        "data/processed/stocks-dividends-docs-processed.csv", index=False
    )
