import pymupdf
import pandas as pd
import os


def get_doc_dividends(doc_path):
    doc = pymupdf.open(doc_path)

    for page in doc:
        tabs = page.find_tables()
        if tabs.tables:
            df_version = pd.DataFrame(tabs[0].extract())

            df = pd.DataFrame(tabs[1].extract())
            df = df.iloc[2:]
            df = df[[0, 1, 6]]
            df.columns = ["TICKER", "VALUE", "DATE"]

            df["DOC_DATE"] = df_version.iat[5, 0]
            df["DOC_VERSION"] = df_version.iat[7, 1]

            return df


def clean_df_dividends(df: pd.DataFrame):
    new_df = df.copy()

    new_df = new_df.replace("\n", "", regex=True)

    new_df["TICKER"] = new_df["TICKER"].str[2:6] + new_df["TICKER"].str[-1]
    new_df["VALUE"] = new_df["VALUE"].str.replace(",", ".").astype(float)
    new_df["DATE"] = pd.to_datetime(new_df["DATE"], dayfirst=True)

    docs_groups = (
        new_df[["DOC_DATE", "DOC_VERSION"]].groupby("DOC_DATE").max().reset_index()
    )

    new_df = new_df.merge(docs_groups, how="inner", on=["DOC_DATE", "DOC_VERSION"])

    new_df = (
        new_df[["TICKER", "DATE", "VALUE"]]
        .sort_values(by=["TICKER", "DATE"])
        .reset_index(drop=True)
    )

    return new_df


def load_dividends_from_pdf():
    df_all_dividends = pd.DataFrame()
    df_docs_processed = pd.DataFrame()

    docs_path = "data/raw/dividends/"
    docs = os.listdir(docs_path)

    for doc in docs:
        doc_path = docs_path + doc
        df_doc_dividends = get_doc_dividends(doc_path=doc_path)
        df_all_dividends = pd.concat([df_all_dividends, df_doc_dividends])
        df_docs_processed = pd.concat(
            [df_docs_processed, pd.DataFrame(data=[doc], columns=["FILE_NAME"])]
        )

    df_all_dividends = clean_df_dividends(df_all_dividends)

    df_all_dividends.to_csv("data/processed/stocks-dividends.csv", index=False)
    df_docs_processed.to_csv(
        "data/processed/stocks-dividends-docs-processed.csv", index=False
    )


load_dividends_from_pdf()
