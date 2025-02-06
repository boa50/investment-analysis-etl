import pandas as pd
from os.path import join
import urllib.request

### Defining urls request agent
opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)


def download_file(filename, url):
    output_path = "data/raw/dividends/"

    print("Downloading {} ...".format(filename))
    try:
        urllib.request.urlretrieve(url, join(output_path, filename))
    except Exception as error:
        print("Error downloading {} ... {}".format(filename, error))


cols = ["Codigo_CVM", "Data_Referencia", "Categoria", "Data_Entrega", "Link_Download"]

df = pd.read_csv(
    "data/raw/ipe_cia_aberta_2024.csv", encoding="ISO-8859-1", sep=";", usecols=cols
)

df = df[df["Categoria"] == "Relatório Proventos"]

df = df.drop("Categoria", axis=1)

df["FILE_NAME"] = df["Codigo_CVM"].astype(str) + "_" + df["Data_Referencia"] + ".pdf"

### For testing purposes
df = df[df["Codigo_CVM"] == 1023]

for index, row in df.iterrows():
    download_file(filename=row["FILE_NAME"], url=row["Link_Download"])
