import zipfile
from os import listdir
from os.path import isfile, join

data_path = "data/processed"


def zip_data():
    data_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    data_files = [f for f in data_files if ".csv" in f]

    with zipfile.ZipFile(
        join(data_path, "data.zip"), "w", zipfile.ZIP_DEFLATED
    ) as zip_data:
        for f in data_files:
            zip_data.write(join(data_path, f))


def unzip_data():
    with zipfile.ZipFile(join(data_path, "data.zip"), "r") as zip_ref:
        zip_ref.extractall("")


zip_data()
# unzip_data()
