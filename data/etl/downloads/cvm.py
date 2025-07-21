import zipfile
from os import listdir
from os.path import isfile, join
from urllib.request import urlretrieve
from pathlib import Path
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import data.etl.queries.queries as qu
import pandas as pd

from io import StringIO
from html.parser import HTMLParser


class _MLStripper(HTMLParser):
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


def _strip_tags(html):
    s = _MLStripper()
    s.feed(html)
    return s.get_data()


def _get_files_to_download(file_data="", years_to_load=[datetime.now().year + 1]):
    df_last_download = qu.get_files_last_download_date(file_data=file_data)
    df_last_download["DATE"] = pd.to_datetime(df_last_download["DATE"])

    url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{file_data.upper()}/DADOS/"
    html = requests.get(url)

    soup = BeautifulSoup(html.content, "html.parser")

    table = soup.find("pre")
    cells = table.prettify().split()

    file_name_base = f"{file_data}_cia_aberta_"

    df_last_update = pd.DataFrame()

    for year in years_to_load:
        position = [
            cells.index(cell) for cell in cells if f"{file_name_base}{year}" in cell
        ]

        if len(position) > 0:
            df_last_update = pd.concat(
                [
                    df_last_update,
                    pd.DataFrame(
                        {
                            "NAME": _strip_tags(f"<{cells[position[0]]}").replace(
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

    df = pd.merge(df_last_download, df_last_update, on="NAME", how="right")

    df_files_to_download = df[(df["DATE_UPDATED"] > df["DATE"]) | (df["DATE"].isna())]

    return df_files_to_download


def _get_data_files(base_filename: str, file_type: str, files_path: str):
    data_files = [f for f in listdir(files_path) if isfile(join(files_path, f))]
    data_files = [f for f in data_files if f"{base_filename[:3]}_cia_aberta_" in f]
    data_files = [f for f in data_files if f"{base_filename[-4:]}.{file_type}" in f]

    return [join(files_path, file) for file in data_files]


def _delete_files(base_filename: str, file_type: str, files_path: str):
    data_files = _get_data_files(
        base_filename=base_filename, file_type=file_type, files_path=files_path
    )

    for file in data_files:
        Path(file).unlink(missing_ok=True)


def _download_zips(files_to_download: list):
    zip_path = "data/raw/zips/"
    Path(zip_path).mkdir(exist_ok=True)

    for filename in files_to_download:
        print(f"Deleting current {filename} zip ...")
        _delete_files(
            base_filename=filename, file_type="zip", files_path="data/raw/zips"
        )

        fname = f"{filename}.zip"
        url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{filename[:3].upper()}/DADOS/{fname}"

        print(f"Downloading {fname} ...")
        try:
            urlretrieve(url, join(zip_path, fname))
        except Exception:
            print(f"Error downloading {fname} ...")


def _delete_unnecessary_files():
    data_path = "data/raw"

    all_csv = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    files_to_exclude = []

    patterns_to_exclude = ["_2", "_DFC_", "_DMPL_", "_DRA_", "_DVA_", "_parecer_"]

    ### FCA files to exclude
    files = [
        f
        for f in all_csv
        if (("fca_cia_aberta_" in f) and ("fca_cia_aberta_valor_mobiliario_" not in f))
    ]
    files_to_exclude.extend(files)

    ### KPIs files to exclude
    for pattern in patterns_to_exclude:
        pattern = "_cia_aberta" + pattern
        files = [f for f in all_csv if (pattern in f) and ("ipe_cia_aberta_" not in f)]
        files_to_exclude.extend(files)

    files_to_exclude.sort()

    for filename in files_to_exclude:
        print(f"Deleting {filename} ...")

        filepath = join(data_path, filename)

        Path(filepath).unlink(missing_ok=True)


def _extract_zips(downloaded_files: list, delete_zips=True):
    for filename in downloaded_files:
        print(f"Deleting current {filename} csvs ...")
        _delete_files(base_filename=filename, file_type="csv", files_path="data/raw")

        data_files = _get_data_files(
            base_filename=filename, file_type="zip", files_path="data/raw/zips"
        )

        for data_file in data_files:
            print(f"Extracting {data_file} ...")

            with zipfile.ZipFile(data_file, "r") as zip_ref:
                zip_ref.extractall("data/raw")

            if delete_zips:
                Path(data_file).unlink(missing_ok=True)

    _delete_unnecessary_files()


def update_files(delete_zips=True):
    df_all_files_updated = pd.DataFrame()

    map_data_years_load = [
        {
            "file_data": "ipe",
            "years_to_load": list(range(2024, datetime.now().year + 1)),
        },
        {
            "file_data": "itr",
            "years_to_load": list(range(2011, datetime.now().year + 1)),
        },
        {
            "file_data": "dfp",
            "years_to_load": list(range(2011, datetime.now().year + 1)),
        },
        {"file_data": "fca", "years_to_load": [2024]},
    ]

    for data in map_data_years_load:
        df_files_to_download = _get_files_to_download(
            file_data=data["file_data"], years_to_load=data["years_to_load"]
        )
        files_to_download = df_files_to_download["NAME"].values
        _download_zips(files_to_download)

        _extract_zips(downloaded_files=files_to_download, delete_zips=delete_zips)

        df_all_files_updated = pd.concat([df_all_files_updated, df_files_to_download])

    df_all_files_updated = df_all_files_updated.drop("DATE", axis=1)
    df_all_files_updated.columns = ["NAME", "DATE"]

    return df_all_files_updated.reset_index(drop=True)


def update_control_table(df_files_updated: pd.DataFrame):
    all_files = qu.get_all_files_download_control()["NAME"].to_list()

    for _, row in df_files_updated.iterrows():
        if row[0] in all_files:
            qu.update_control_table(filename=row[0], date=row[1].date())
        else:
            qu.insert_on_control_table(filename=row[0], date=row[1].date())
