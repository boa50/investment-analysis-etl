import zipfile
from os import listdir
from os.path import isfile, join

data_path = "data/raw/zips"

data_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
data_files = [f for f in data_files if ".zip" in f]
data_files.sort()

for data_file in data_files:
    print("Extracting " + data_file + " ...")

    with zipfile.ZipFile(join(data_path, data_file), "r") as zip_ref:
        zip_ref.extractall("data/raw")
