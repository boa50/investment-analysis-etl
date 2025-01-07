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
        
    else: 
        return []