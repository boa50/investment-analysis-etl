from data.db_creation import batch_load
import pandas as pd
from pathlib import Path

### DIVIDENDS
df = pd.read_csv("data/processed/stocks-dividends.csv")
batch_load.load_data(table_name="stocks-dividends", df=df)
Path("data/processed/stocks-dividends.csv").unlink(missing_ok=True)

### DIVIDENDS DOCS PROCESSED
df = pd.read_csv("data/processed/stocks-dividends-docs-processed.csv")
batch_load.load_data(table_name="stocks-dividends-docs-processed", df=df)
Path("data/processed/stocks-dividends-docs-processed.csv").unlink(missing_ok=True)

### CUSTOM DIVIDENDS (I STILL DON'T KNOW FROM WHERE THEY ARE COMMING)
# df = pd.DataFrame(
#     data=[["BBAS3", "2024-08-30", 0.00270692], ["BBAS3", "2024-08-30", 0.00560564]],
#     columns=["TICKER", "DATE", "VALUE"],
# )
# batch_load.load_data(table_name="stocks-dividends", df=df)
