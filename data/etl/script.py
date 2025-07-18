import data.etl.downloads.cvm as cvm_download

import data.etl.prices.data_download as prices_data_download
import data.etl.prices.queries as prices_queries
import data.etl.prices.db_load as prices_db_load

import data.etl.dividends.pdf_download as dividends_pdf_download
import data.etl.dividends.pdf_load as dividends_pdf_load
import data.etl.dividends.db_load as dividends_db_load

import data.etl.fundaments.script as fundaments
import data.etl.stocks_in_file as stocks_in_file

df_files_updated = cvm_download.update_files()

### To regenerate the stocks files list
# stocks_in_file.create_stocks_files_list()

### Prices
try:
    prices_data_download.get_latest_prices()
    prices_queries.delete_outdated_prices()
    prices_db_load.load_prices_into_db()

    print("Prices updated")
except FileNotFoundError:
    print("Prices file doesn't exist")

### Dividends
dividends_pdf_download.download_pdfs()

pdf_load_return = dividends_pdf_load.load_dividends_from_pdf()

if pdf_load_return:
    dividends_db_load.load_dividends_into_db()
    dividends_db_load.load_dividends_docs_into_db()
    dividends_db_load.load_custom_dividends_into_db()

    print("Dividends updated")
else:
    print("No dividends to update")

### Fundaments
fundaments_files_updated = [
    f
    for f in df_files_updated["NAME"].values
    if ("itr_cia_aberta_" in f) or ("dfp_cia_aberta_" in f)
]

if len(fundaments_files_updated) > 0:
    years_to_load = list(set([int(f[-4:]) for f in fundaments_files_updated]))
    fundaments.process_files_to_csv()
    fundaments.update_database(years_to_update=years_to_load)

    print(f"Fundaments updated for the years {years_to_load}")
else:
    print("No fundaments to update")


cvm_download.update_control_table(df_files_updated=df_files_updated)
