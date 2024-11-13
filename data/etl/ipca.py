import pandas as pd


def process_raw_file():
    df = pd.read_json("data/raw/ipca.json")

    ### Getting only the Número Índices
    df = df[df["MC"] == "30"]

    df = df[["D3C", "V"]]

    df.columns = ["DATE", "VALUE"]

    df = df[df["DATE"] >= "201401"]

    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m").dt.to_period("M")

    df["VALUE"] = df["VALUE"].astype(float)

    print(df)

    df.to_csv("data/processed/ipca.csv", index=False)


process_raw_file()
