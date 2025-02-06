import pymupdf
import pandas as pd
import os


def get_doc_dividends(doc_path):
    doc = pymupdf.open(doc_path)

    for page in doc:
        tabs = page.find_tables()
        if tabs.tables:
            df = pd.DataFrame(tabs[1].extract())
            df = df.iloc[2:]
            df = df[[0, 1, 6]]
            df.columns = ["TICKER", "VALUE", "DATE"]

    return df


def clean_df_dividends(df):
    new_df = df.copy()

    new_df = new_df.replace("\n", "", regex=True)

    new_df["TICKER"] = new_df["TICKER"].str[2:6] + new_df["TICKER"].str[-1]
    new_df["VALUE"] = new_df["VALUE"].str.replace(",", ".").astype(float)
    new_df["DATE"] = pd.to_datetime(new_df["DATE"], dayfirst=True)

    new_df = new_df[["TICKER", "DATE", "VALUE"]].reset_index(drop=True)

    return new_df


def load_dividends_from_pdf():
    df_all_dividends = pd.DataFrame()

    docs_path = "data/raw/dividends/"
    docs = os.listdir(docs_path)

    for doc in docs:
        doc_path = docs_path + doc
        df_doc_dividends = get_doc_dividends(doc_path=doc_path)
        df_all_dividends = pd.concat([df_all_dividends, df_doc_dividends])

    df_all_dividends = clean_df_dividends(df_all_dividends)

    return df_all_dividends
