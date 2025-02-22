import zipfile
from os import listdir
from os.path import isfile, join
from urllib.request import urlretrieve
from pathlib import Path
from datetime import datetime


def download_zips(year_initial=2011, year_final=datetime.now().year):
    data_path = "data/raw/zips/"
    Path(data_path).mkdir(exist_ok=True)

    print(year_final)

    def get_filename_and_url(type, year):
        filename = "{}_cia_aberta_{}.zip".format(type, year)
        url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{}/DADOS/{}".format(
            type.upper(), filename
        )

        return [filename, url]

    def download_file(type, year):
        [filename, url] = get_filename_and_url(type, year)
        print("Downloading {} ...".format(filename))
        try:
            urlretrieve(url, join(data_path, filename))
        except Exception:
            print("Error downloading {} ...".format(filename))

    # Registration data
    download_file("fca", year_final)

    # KPIs data
    years_load = list(range(year_initial, year_final + 1))
    types_load = ["itr", "dfp"]

    for year in years_load:
        for type in types_load:
            download_file(type, year)


def delete_unnecessary_files(delete_zip=None):
    data_path = "data/raw"

    all_csv = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    files_to_exclude = []

    patterns_to_exclude = ["_2", "_DFC_", "_DMPL_", "_DRA_", "_DVA_", "_parecer_"]

    # FCA files to exclude
    files = [
        f
        for f in all_csv
        if (("fca_cia_aberta_" in f) and ("fca_cia_aberta_valor_mobiliario_" not in f))
    ]
    files_to_exclude.extend(files)

    # # KPIs files to exclude
    for pattern in patterns_to_exclude:
        pattern = "_cia_aberta" + pattern
        files = [f for f in all_csv if pattern in f]
        files_to_exclude.extend(files)

    files_to_exclude.sort()

    for filename in files_to_exclude:
        print("Deleting {} ...".format(filename))

        filepath = join(data_path, filename)

        Path(filepath).unlink(missing_ok=True)

    if delete_zip is not None:
        print("Deleting {} ...".format(delete_zip))

        Path(delete_zip).unlink(missing_ok=True)


def extract_zips(delete_zips=False):
    data_path = "data/raw/zips"

    data_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    data_files = [f for f in data_files if ".zip" in f]
    data_files.sort()

    for data_file in data_files:
        print("Extracting " + data_file + " ...")

        zip_path = join(data_path, data_file)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall("data/raw")

        if delete_zips:
            delete_unnecessary_files(zip_path)
        else:
            delete_unnecessary_files()

    if delete_zips:
        Path(data_path).rmdir()


# download_zips()
# extract_zips(delete_zips=True)
