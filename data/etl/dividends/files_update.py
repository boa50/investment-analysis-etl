import pandas as pd
import queries

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
import zipfile
from os import listdir
from os.path import isfile, join
from urllib.request import urlretrieve

from io import StringIO
from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def _get_files_to_download():
    df_last_download = queries.get_ipe_files_last_download_date()
    df_last_download["DATE"] = pd.to_datetime(df_last_download["DATE"])

    url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/"
    html = requests.get(url)

    soup = BeautifulSoup(html.content, "html.parser")

    table = soup.find("pre")
    cells = table.prettify().split()

    years_load = list(range(2024, datetime.now().year + 1))

    file_name_base = "ipe_cia_aberta_"

    df_last_update = pd.DataFrame()

    for year in years_load:
        position = [
            cells.index(cell) for cell in cells if f"{file_name_base}{year}" in cell
        ]

        df_last_update = pd.concat(
            [
                df_last_update,
                pd.DataFrame(
                    {
                        "NAME": strip_tags(f"<{cells[position[0]]}").replace(
                            ".zip", ""
                        ),
                        "DATE_UPDATED": cells[position[0] + 1],
                    },
                    index=[0],
                ),
            ]
        )

    df_last_update["DATE_UPDATED"] = pd.to_datetime(df_last_update["DATE_UPDATED"])
    df_last_update = df_last_update.reset_index(drop=True)

    df = pd.merge(df_last_download, df_last_update, on="NAME")

    df_files_to_download = df[df["DATE_UPDATED"] > df["DATE"]]

    return df_files_to_download


def _download_zips(files_to_download: list):
    zip_path = "data/raw/zips/"
    Path(zip_path).mkdir(exist_ok=True)

    for filename in files_to_download:
        print(f"Deleting current {filename} ...")
        Path(f"data/raw/{filename}.csv").unlink(missing_ok=True)

        fname = f"{filename}.zip"
        url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/{fname}"

        print(f"Downloading {fname} ...")
        try:
            urlretrieve(url, join(zip_path, fname))
        except Exception:
            print(f"Error downloading {fname} ...")


def _extract_zips(delete_zips=True):
    zips_path = "data/raw/zips"

    data_files = [f for f in listdir(zips_path) if isfile(join(zips_path, f))]
    data_files = [f for f in data_files if ".zip" in f]
    data_files = [f for f in data_files if "ipe_cia_aberta_" in f]
    data_files.sort()

    for data_file in data_files:
        print(f"Extracting {data_file} ...")

        zip_path = join(zips_path, data_file)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall("data/raw")

        if delete_zips:
            Path(zip_path).unlink(missing_ok=True)


def update_files():
    df_files_to_download = _get_files_to_download()
    files_to_download = df_files_to_download["NAME"].values
    _download_zips(files_to_download)
    _extract_zips()

    df_files_to_download = df_files_to_download.drop("DATE", axis=1)
    df_files_to_download.columns = ["NAME", "DATE"]

    return df_files_to_download


def update_control_table(df_files_updated: pd.DataFrame):
    for _, row in df_files_updated.iterrows():
        queries.update_control_table(filename=row[0], date=row[1].date())
