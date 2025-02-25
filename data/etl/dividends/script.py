import pdf_download
import pdf_load
import db_load
import files_update

df_files_updated = files_update.update_files()
pdf_download.download_pdfs()

pdf_load_return = pdf_load.load_dividends_from_pdf()

if pdf_load_return:
    db_load.load_dividends_into_db()
    db_load.load_dividends_docs_into_db()
    db_load.load_custom_dividends_into_db()
    files_update.update_control_table(df_files_updated)

    print("Dividends updated")
else:
    print("No dividends to update")
