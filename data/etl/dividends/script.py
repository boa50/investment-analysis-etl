import pdf_download
import pdf_load
import db_load

pdf_download.download_pdfs()
pdf_load.load_dividends_from_pdf()
db_load.load_dividends_into_db()
db_load.load_dividends_docs_into_db()
db_load.load_custom_dividends_into_db()

# import pandas as pd
# from datetime import datetime
# from utils import get_main_ticker

# df_dividends = pd.DataFrame(columns=["CD_CVM", "TICKER", "DATE", "VALUE", "TYPE"])

# ### Getting data related to stocks splitting
# df_splits = pd.read_csv("data/processed/stocks-splits.csv")
# df_splits["DATE"] = pd.to_datetime(df_splits["DATE"]).dt.date


# ### Getting only companies available on basic info file
# df_basic_info = pd.read_csv("data/processed/stocks-basic-info.csv")

# for idx in range(df_basic_info.shape[0]):
#     company = df_basic_info.iloc[idx]
#     ticker = get_main_ticker(company["TICKERS"])
#     ticker_base_code = ticker[:4]
#     ticker_stock_type = "ON" if ticker[-1] == "3" else "PN"

#     print(f"Updating dividends for {ticker} CD_CVM: {company['CD_CVM']}")

#     tk_dividends = pd.read_csv(f"data/raw/_dividends_{ticker_base_code}.csv")

#     ### Must implement this function later
#     # update_dividends(cd_cvm, ticker_base_code)

#     tk_dividends = tk_dividends[tk_dividends["STOCK_TYPE"] == ticker_stock_type]
#     tk_dividends = tk_dividends.drop("STOCK_TYPE", axis=1)

#     tk_dividends["DATE"] = pd.to_datetime(
#         tk_dividends["DATE"], format="%d/%m/%Y"
#     ).dt.date

#     # Getting only last 11 years of dividends to make it easier to calculate last 10 years KPIs
#     last_date = (datetime.today() - pd.DateOffset(years=11)).date()

#     tk_dividends = tk_dividends[tk_dividends["DATE"] >= last_date]

#     tk_dividends["TICKER"] = ticker
#     tk_dividends["CD_CVM"] = company["CD_CVM"]

#     df_splits_tk = df_splits[df_splits["CD_CVM"] == company["CD_CVM"]]
#     for _, row in df_splits_tk.iterrows():
#         tk_dividends.loc[tk_dividends["DATE"] <= row["DATE"], "VALUE"] /= row[
#             "PROPORTION"
#         ]

#     df_dividends = pd.concat([df_dividends, tk_dividends])

# print(df_dividends.tail())

# df_dividends.to_csv("data/processed/stocks-dividends.csv", index=False)
