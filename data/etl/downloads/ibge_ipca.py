from urllib.request import urlretrieve
from pathlib import Path
import pandas as pd


def _process_raw_file(filepath):
    df = pd.read_json(filepath)

    ### Getting only the Número Índices
    df = df[df["MC"] == "30"]

    df = df[["D3C", "V"]]

    df.columns = ["DATE", "VALUE"]

    df = df[df["DATE"] >= "201401"]

    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m").dt.to_period("M")

    df["VALUE"] = df["VALUE"].astype(float)

    print(df)

    df.to_csv("data/processed/ipca.csv", index=False)


def download_ipca_file():
    url = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2266%2013,v2263%202,v2264%202,v2265%202?formato=json"

    filepath = "data/raw/ipca.json"

    urlretrieve(url, filepath)

    _process_raw_file(filepath=filepath)

    Path(filepath).unlink(missing_ok=True)


download_ipca_file()
