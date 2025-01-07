from google.cloud import bigquery

def get_schema(table_name):
    if table_name == "stocks-fundaments":
        return [
                bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("DT_START", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("DT_END", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("DT_YEAR", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("KPI", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("VALUE", "BIGDECIMAL", mode="NULLABLE"),
                bigquery.SchemaField("VALUE_ROLLING_YEAR", "BIGDECIMAL", mode="NULLABLE"),
            ]
    elif table_name == "stocks-history":
        return [
                bigquery.SchemaField("DT_EVENT", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("CD_CVM", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("TICKER", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("PRICE", "BIGDECIMAL", mode="NULLABLE"),
                bigquery.SchemaField("PRICE_PROFIT", "BIGDECIMAL", mode="NULLABLE"),
                bigquery.SchemaField("DIVIDEND_YIELD", "BIGDECIMAL", mode="NULLABLE"),
                bigquery.SchemaField("DIVIDEND_PAYOUT", "BIGDECIMAL", mode="NULLABLE"),
                bigquery.SchemaField("PRICE_EQUITY", "BIGDECIMAL", mode="NULLABLE"),
            ]
    else: 
        return []