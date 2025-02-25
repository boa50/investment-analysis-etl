import pdf_download
import pdf_load
import db_load

pdf_download.download_pdfs()
pdf_load.load_dividends_from_pdf()
db_load.load_dividends_into_db()
db_load.load_dividends_docs_into_db()
db_load.load_custom_dividends_into_db()
