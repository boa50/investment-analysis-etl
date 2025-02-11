import pandas as pd
import os
import urllib.request
import queries

### Defining urls request agent
opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)


def download_file(filename, url):
    output_path = "data/raw/dividends/"

    print("Downloading {} ...".format(filename))
    try:
        urllib.request.urlretrieve(url, os.path.join(output_path, filename))
    except Exception as error:
        print("Error downloading {} ... {}".format(filename, error))


def create_file_names(df):
    protocol_text = "&numProtocolo="
    sequence_text = "&numSequencia="
    version_text = "&numVersao="

    protocol_position = df["Link_Download"].iloc[0].find(protocol_text)
    sequence_position = df["Link_Download"].iloc[0].find(sequence_text)
    version_position = df["Link_Download"].iloc[0].find(version_text)

    df["FILE_PROTOCOL"] = df["Link_Download"].str[
        protocol_position + len(protocol_text) : sequence_position
    ]
    df["FILE_SEQUENCE"] = df["Link_Download"].str[
        sequence_position + len(sequence_text) : version_position
    ]
    df["FILE_VERSION"] = df["Link_Download"].str[-1]

    df["FILE_NAME"] = (
        df["Codigo_CVM"].astype(str)
        + "_"
        + df["Data_Referencia"]
        + "_"
        + df["FILE_PROTOCOL"]
        + "_"
        + df["FILE_SEQUENCE"]
        + "_"
        + df["FILE_VERSION"]
        + ".pdf"
    )

    df = df.drop(["FILE_PROTOCOL", "FILE_SEQUENCE", "FILE_VERSION"], axis=1)

    return df["FILE_NAME"]


def process_ipe_file(document_year):
    file_name = f"ipe_cia_aberta_{document_year}.csv"

    print()
    print(f"Processing {file_name}")

    cols = [
        "Codigo_CVM",
        "Data_Referencia",
        "Categoria",
        "Data_Entrega",
        "Link_Download",
    ]

    df = pd.read_csv(
        f"data/raw/{file_name}",
        encoding="ISO-8859-1",
        sep=";",
        usecols=cols,
    )

    df = df[df["Categoria"] == "Relatório Proventos"]

    df = df.drop("Categoria", axis=1)

    df["FILE_NAME"] = create_file_names(df)

    available_cds_cvm = [int(n) for n in queries.get_available_cds_cvm()]

    df = df[df["Codigo_CVM"].isin(available_cds_cvm)]

    already_downloaded_files = os.listdir("data/raw/dividends/")

    for index, row in df.iterrows():
        file_name = row["FILE_NAME"]

        if file_name not in already_downloaded_files:
            print(f"Downloading {file_name}")
            download_file(filename=file_name, url=row["Link_Download"])


docs_path = "data/raw/"
docs = os.listdir(docs_path)
docs = [f for f in docs if "ipe_cia_aberta_" in f]
documents_years = [f[-8:-4] for f in docs]
documents_years.sort()

for year in documents_years:
    process_ipe_file(document_year=year)
