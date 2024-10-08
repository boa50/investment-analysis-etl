import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random


def get_stock_info(cd_cvm):
    print("Getting stock info from bovespa for CD_CVM = " + str(cd_cvm))

    time.sleep(random.randint(1, 7))

    url = (
        "https://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM="
        + str(cd_cvm)
    )
    page = requests.get(url, verify=False)

    soup = BeautifulSoup(page.content, "html.parser")

    tickers = soup.find(id="accordionDados").find_all(
        "a", class_="LinkCodNeg", attrs={"href": "javascript:;"}
    )

    num_stocks = soup.find(id="divComposicaoCapitalSocial").find_all(
        "td", class_="text-right"
    )

    return {
        "CD_CVM": cd_cvm,
        "TICKERS": ";".join([ticker.text for ticker in tickers]),
        "NUM_ORDINARIAS": int(num_stocks[0].text.replace(".", "")),
        "NUM_PREFERENCIAIS": int(num_stocks[1].text.replace(".", "")),
        "NUM_TOTAL": int(num_stocks[2].text.replace(".", "")),
    }


df = pd.read_csv("data/raw/cad_cia_aberta.csv", encoding="ISO-8859-1", sep=";")

df = df[(df["SIT"] == "ATIVO")]

columns = ["CD_CVM", "DENOM_SOCIAL", "CNPJ_CIA", "SETOR_ATIV", "CONTROLE_ACIONARIO"]

cd_cvm_test = [
    1023,  # Banco do Brasil
    906,  # Bradesco
    20257,  # Taesa
    18376,  # Transmissões Paulista
]

df = df[df["CD_CVM"].isin(cd_cvm_test)][columns]
df = df.drop_duplicates()

ticker_df = pd.DataFrame(
    columns=["CD_CVM", "TICKERS", "NUM_ORDINARIAS", "NUM_PREFERENCIAIS", "NUM_TOTAL"]
)

for cd_cvm in df["CD_CVM"].unique():
    df_tmp = pd.DataFrame(get_stock_info(cd_cvm), index=[ticker_df.shape[0]])
    ticker_df = pd.concat([ticker_df, df_tmp])

df = pd.merge(df, ticker_df, on="CD_CVM")

print(df.head())

df.to_csv("data/processed/stocks-basic-info.csv", index=False)
