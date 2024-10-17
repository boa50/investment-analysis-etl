import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random


# def get_stock_info(cd_cvm):
#     print("Getting stock info from bovespa for CD_CVM = " + str(cd_cvm))

#     time.sleep(random.randint(1, 7))

#     url = (
#         "https://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM="
#         + str(cd_cvm)
#     )
#     page = requests.get(url, verify=False)

#     soup = BeautifulSoup(page.content, "html.parser")

#     tickers = soup.find(id="accordionDados").find_all(
#         "a", class_="LinkCodNeg", attrs={"href": "javascript:;"}
#     )

#     num_stocks = soup.find(id="divComposicaoCapitalSocial").find_all(
#         "td", class_="text-right"
#     )

#     return {
#         "CD_CVM": cd_cvm,
#         "TICKERS": ";".join([ticker.text for ticker in tickers]),
#         "NUM_ORDINARIAS": int(num_stocks[0].text.replace(".", "")),
#         "NUM_PREFERENCIAIS": int(num_stocks[1].text.replace(".", "")),
#         "NUM_TOTAL": int(num_stocks[2].text.replace(".", "")),
#     }


### Doing it manually to avoid making more requests
def get_all_stock_info():
    def create_ticker_df(cd_cvm, tickers, num_ordinarias, num_preferenciais):
        return pd.DataFrame(
            {
                "CD_CVM": cd_cvm,
                "TICKERS": tickers,
                "NUM_ORDINARIAS": num_ordinarias,
                "NUM_PREFERENCIAIS": num_preferenciais,
                "NUM_TOTAL": num_ordinarias + num_preferenciais,
            },
            index=[0],
        )

    df = pd.concat(
        [
            # Data obtained from https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/1023/overview/overview?language=pt-br
            create_ticker_df(906, "BBDC3;BBDC4", 5330304681, 5311865547),
            create_ticker_df(1023, "BBAS11;BBAS12;BBAS3", 5730834040, 0),
            create_ticker_df(18376, "TRPL3;TRPL4", 257937732, 400945572),
            create_ticker_df(20257, "TAEE11;TAEE3;TAEE4", 590714069, 442782652),
        ]
    )

    df = df.reset_index(drop=True)

    return df


df = pd.read_csv("data/raw/cad_cia_aberta.csv", encoding="ISO-8859-1", sep=";")

df = df[(df["SIT"] == "ATIVO")]

columns = ["CD_CVM", "DENOM_SOCIAL", "CNPJ_CIA", "CONTROLE_ACIONARIO"]

cd_cvm_test = [
    1023,  # Banco do Brasil
    906,  # Bradesco
    20257,  # Taesa
    18376,  # Transmissões Paulista
    ### NEW ONES ###
    19348,  # Itaú
    922,  # Banco da Amazônia
    22616,  # Banco BTG
    20532,  # Santander
    2453,  # CEMIG
    18660,  # CPFL Energias
    2437,  # Eletrobras
    20010,  # Equatorial Energia
    17329,  # ENGIE
    15539,  # NeoEnergia
]

df = df[df["CD_CVM"].isin(cd_cvm_test)][columns]
df = df.drop_duplicates()

# ticker_df = pd.DataFrame(
#     columns=["CD_CVM", "TICKERS", "NUM_ORDINARIAS", "NUM_PREFERENCIAIS", "NUM_TOTAL"]
# )

# for cd_cvm in df["CD_CVM"].unique():
#     df_tmp = pd.DataFrame(get_stock_info(cd_cvm), index=[ticker_df.shape[0]])
#     ticker_df = pd.concat([ticker_df, df_tmp])

ticker_df = get_all_stock_info()

df = pd.merge(df, ticker_df, on="CD_CVM")

print(df.head())

df.to_csv("data/processed/stocks-basic-info.csv", index=False)
