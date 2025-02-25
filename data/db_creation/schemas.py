from google.cloud import bigquery


def get_schema(table_name):
    if table_name == "stocks-fundaments":
        return [
            bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DT_START", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("DT_END", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("DT_YEAR", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("KPI", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("VALUE", "BIGDECIMAL", mode="NULLABLE"),
            bigquery.SchemaField("VALUE_ROLLING_YEAR", "BIGDECIMAL", mode="NULLABLE"),
        ]
    elif table_name == "stocks-history":
        return [
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("PRICE", "BIGDECIMAL", mode="NULLABLE"),
            bigquery.SchemaField("PRICE_PROFIT", "BIGDECIMAL", mode="NULLABLE"),
            bigquery.SchemaField("DIVIDEND_YIELD", "BIGDECIMAL", mode="NULLABLE"),
            bigquery.SchemaField("DIVIDEND_PAYOUT", "BIGDECIMAL", mode="NULLABLE"),
            bigquery.SchemaField("PRICE_EQUITY", "BIGDECIMAL", mode="NULLABLE"),
        ]
    elif table_name == "stocks-basic-info":
        return [
            bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("NAME", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("TICKERS", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("NUM_COMMON", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("NUM_PREFERENTIAL", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("NUM_TOTAL", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("AVAILABLE_COMMON", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("AVAILABLE_PREFERENTIAL", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("AVAILABLE_TOTAL", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("FOUNDATION", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("GOVERNANCE_LEVEL", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("SECTOR", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("SUBSECTOR", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("SEGMENT", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("WEB_PAGE", "STRING", mode="NULLABLE"),
        ]
    elif table_name == "stocks-right-prices":
        return [
            bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("BAZIN", "BIGDECIMAL", mode="NULLABLE"),
        ]
    elif table_name == "ipca":
        return [
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("VALUE", "DECIMAL", mode="REQUIRED"),
        ]
    elif table_name == "stocks-ratings":
        return [
            bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("VALUE", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("DEBT", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("EFFICIENCY", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("GROWTH", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("OVERALL", "BIGDECIMAL", mode="REQUIRED"),
        ]
    elif table_name == "stocks-dividends":
        return [
            bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("VALUE", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("DOC_DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("VERSION", "INTEGER", mode="REQUIRED"),
        ]
    elif table_name == "stocks-dividends-docs-processed":
        return [bigquery.SchemaField("FILE_NAME", "STRING", mode="REQUIRED")]
    elif table_name == "stocks-dividends-old":
        return [
            bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("VALUE", "BIGDECIMAL", mode="REQUIRED"),
            bigquery.SchemaField("TYPE", "STRING", mode="NULLABLE"),
        ]
    elif table_name == "stocks-splits":
        return [
            bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("PROPORTION", "DECIMAL", mode="REQUIRED"),
        ]
    elif table_name == "files-download-control":
        return [
            bigquery.SchemaField("NAME", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"),
        ]
    else:
        return []
