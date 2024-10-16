import zipfile
from os import listdir
from os.path import isfile, join
from urllib.request import urlretrieve
from pathlib import Path


def download_zips(year_initial=2011, year_final=2024):
    data_path = "data/raw/zips/"
    Path(data_path).mkdir(exist_ok=True)

    years_load = list(range(year_initial, year_final + 1))
    types_load = ["itr", "dfp"]

    def get_filename_and_url(type, year):
        filename = "{}_cia_aberta_{}.zip".format(type, year)
        url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{}/DADOS/{}".format(
            type.upper(), filename
        )

        return [filename, url]

    for year in years_load:
        for type in types_load:
            [filename, url] = get_filename_and_url(type, year)

            print("Downloading {} ...".format(filename))

            urlretrieve(url, data_path + filename)


def extract_zips():
    data_path = "data/raw/zips"

    data_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    data_files = [f for f in data_files if ".zip" in f]
    data_files.sort()

    for data_file in data_files:
        print("Extracting " + data_file + " ...")

        with zipfile.ZipFile(join(data_path, data_file), "r") as zip_ref:
            zip_ref.extractall("data/raw")


# download_zips()
# extract_zips()
