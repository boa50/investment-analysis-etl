import pandas as pd
from datetime import datetime

df_fca_manual = pd.DataFrame(
    [
        {
            "CNPJ_Companhia": "90.400.888/0001-42",
            "Valor_Mobiliario": "Ações Ordinárias",
            "Codigo_Negociacao": "SANB3",
        },
        {
            "CNPJ_Companhia": "90.400.888/0001-42",
            "Valor_Mobiliario": "Ações Preferenciais",
            "Codigo_Negociacao": "SANB4",
        },
        {
            "CNPJ_Companhia": "30.306.294/0001-45",
            "Valor_Mobiliario": "Ações Ordinárias",
            "Codigo_Negociacao": "BPAC3",
        },
        {
            "CNPJ_Companhia": "30.306.294/0001-45",
            "Valor_Mobiliario": "Ações Preferenciais",
            "Codigo_Negociacao": "BPAC5",
        },
        {
            "CNPJ_Companhia": "30.306.294/0001-45",
            "Valor_Mobiliario": "Units",
            "Codigo_Negociacao": "BPAC11",
        },
    ],
)


### Doing it manually to avoid making more requests
def get_all_stock_info():
    def create_ticker_df(
        cd_cvm,
        num_common,
        num_preferential,
        foundation,
        available_common,
        available_preferential,
        web_page,
    ):
        return pd.DataFrame(
            {
                "CD_CVM": cd_cvm,
                "NUM_COMMON": num_common,
                "NUM_PREFERENTIAL": num_preferential,
                "NUM_TOTAL": num_common + num_preferential,
                "FOUNDATION": foundation,
                "AVAILABLE_COMMON": available_common,
                "AVAILABLE_PREFERENTIAL": available_preferential,
                "AVAILABLE_TOTAL": available_common + available_preferential,
                "WEB_PAGE": web_page,
            },
            index=[0],
        )

    df = pd.concat(
        [
            # Data obtained from https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/1023/overview/overview?language=pt-br
            create_ticker_df(
                906,
                5330304681,
                5311865547,
                1943,
                1475064981,
                5117474436,
                "https://www.bradescori.com.br/",
            ),
            create_ticker_df(
                1023, 5730834040, 0, 1808, 2842613858, 0, "https://ri.bb.com.br/"
            ),
            create_ticker_df(
                18376,
                257937732,
                400945572,
                1999,
                27080900,
                395801044,
                "https://ri.isaenergiabrasil.com.br/",
            ),
            create_ticker_df(
                20257,
                590714069,
                442782652,
                2006,
                218568274,
                437136468,
                "https://ri.taesa.com.br/",
            ),
            create_ticker_df(
                19348,
                4958290359,
                4845844989,
                1943,
                402586896,
                4792902422,
                "https://www.itau.com.br/relacoes-com-investidores/",
            ),
            create_ticker_df(
                2453,
                956601911,
                1905179984,
                1952,
                469002659,
                1858636840,
                "https://ri.cemig.com.br/",
            ),
            create_ticker_df(
                18660, 1152254440, 0, 1998, 187732538, 0, "https://ri.cpfl.com.br/"
            ),
            create_ticker_df(
                19445, 380253069, 0, 1963, 188462398, 0, "https://ri.copasa.com.br/"
            ),
            create_ticker_df(
                18627,
                503735259,
                1007470260,
                1963,
                201077484,
                1007454257,
                "https://ri.sanepar.com.br/",
            ),
            create_ticker_df(
                14443, 683509869, 0, 1973, 683495706, 0, "https://ri.sabesp.com.br/"
            ),
            create_ticker_df(
                922, 56058315, 0, 1942, 1678339, 0, "https://ri.bancoamazonia.com.br/"
            ),
            create_ticker_df(
                22616,
                7244165568,
                4261954360,
                1983,
                1287247964,
                2574884708,
                "https://ri.btgpactual.com/",
            ),
            create_ticker_df(
                20532,
                3818695031,
                3679836020,
                1982,
                356586730,
                384391141,
                "https://www.santander.com.br/ri/home",
            ),
            create_ticker_df(
                2437,
                2027011498,
                280088314,
                1962,
                1977170723,
                268875696,
                "https://ri.eletrobras.com/",
            ),
            create_ticker_df(
                20010,
                1248075298,
                0,
                1999,
                1249258717,
                0,
                "https://ri.equatorialenergia.com.br/",
            ),
            create_ticker_df(
                17329,
                815927740,
                0,
                1998,
                255236938,
                0,
                "https://www.engie.com.br/investidores/",
            ),
            create_ticker_df(
                15539, 1213797248, 0, 1997, 195036523, 0, "https://ri.neoenergia.com/"
            ),
            create_ticker_df(
                24961, 167041869, 0, 1995, 37291589, 0, "https://ri.ambipar.com/"
            ),
            create_ticker_df(
                16861,
                572078479,
                578578081,
                1970,
                61617,
                50071,
                "https://ri.casan.com.br/",
            ),
            create_ticker_df(
                25550, 82950889, 0, 2009, 46689969, 0, "https://ri.orizonvr.com.br/"
            ),
            create_ticker_df(
                25950, 498297647, 0, 1995, 112212186, 0, "https://ri.3tentos.com.br/"
            ),
            create_ticker_df(
                25704,
                135322144,
                0,
                2009,
                46233405,
                0,
                "https://ri.boasafrasementes.com.br/",
            ),
            create_ticker_df(
                20036, 102683444, 0, 2005, 63480488, 0, "https://ri.brasil-agro.com/"
            ),
            create_ticker_df(
                20745,
                443329716,
                0,
                1977,
                194261422,
                0,
                "https://ri.slcagricola.com.br/",
            ),
            create_ticker_df(
                26000,
                96226962,
                0,
                2006,
                52520370,
                0,
                "https://www.terrasantapa.com.br/",
            ),
            create_ticker_df(
                24627, 179393939, 0, 1987, 29680739, 0, "https://ri.blau.com/"
            ),
            create_ticker_df(
                9342, 150377481, 0, 1967, 71516013, 0, "https://ri.grupopanvel.com.br/"
            ),
            create_ticker_df(
                21431, 633420823, 0, 2001, 300837354, 0, "https://ri.hypera.com.br/"
            ),
            create_ticker_df(
                22608, 581715639, 0, 1981, 171119625, 0, "https://ri.paguemenos.com.br/"
            ),
            create_ticker_df(
                5258, 1718007200, 0, 1935, 1289844359, 0, "https://ri.rdsaude.com.br/"
            ),
            create_ticker_df(
                19305, 126841481, 0, 2001, 88057573, 0, "https://ri.biomm.com/"
            ),
            create_ticker_df(
                23507, 53949006, 0, 1987, 23165733, 0, "https://ri.ourofino.com/"
            ),
        ]
    )

    df = df.reset_index(drop=True)

    return df


df = pd.read_csv(
    "data/raw/cad_cia_aberta.csv",
    encoding="ISO-8859-1",
    sep=";",
    usecols=["CNPJ_CIA", "CD_CVM", "DENOM_COMERC", "DENOM_SOCIAL", "SIT"],
)

df = df[(df["SIT"] == "ATIVO")]

ticker_df = get_all_stock_info()

df = df.drop("SIT", axis=1)
df = pd.merge(df, ticker_df, on="CD_CVM")

df = df.drop_duplicates()

### Getting Tickers and Governance Level
df_fca = pd.read_csv(
    "data/raw/fca_cia_aberta_valor_mobiliario_" + str(datetime.now().year) + ".csv",
    encoding="ISO-8859-1",
    sep=";",
    usecols=["CNPJ_Companhia", "Valor_Mobiliario", "Codigo_Negociacao", "Segmento"],
)

# Filling data not existent in the original dataframe
df_fca = pd.concat([df_fca, df_fca_manual])

df_fca["Segmento"] = df_fca.groupby("CNPJ_Companhia")["Segmento"].ffill()

df_fca = df_fca[
    df_fca["CNPJ_Companhia"].isin(df["CNPJ_CIA"].values)
    & df_fca["Valor_Mobiliario"].isin(
        ["Ações Ordinárias", "Ações Preferenciais", "Units"]
    )
    & df_fca["Codigo_Negociacao"].notna()
    & (df_fca["Codigo_Negociacao"] != "000000")
]
df_fca = df_fca.drop("Valor_Mobiliario", axis=1)
df_fca = df_fca.groupby("CNPJ_Companhia", as_index=False).agg(
    {"Codigo_Negociacao": ";".join, "Segmento": "first"}
)
df_fca["Segmento"] = df_fca["Segmento"].str.replace(" de Governança Corporativa", "")
df_fca = df_fca.rename(
    columns={
        "CNPJ_Companhia": "CNPJ_CIA",
        "Codigo_Negociacao": "TICKERS",
        "Segmento": "GOVERNANCE_LEVEL",
    }
)

df = df.merge(df_fca, how="left", on="CNPJ_CIA")
df = df.drop("CNPJ_CIA", axis=1)

### Getting B3 sectors
df["CODIGO"] = df["TICKERS"].str[:4]

df_sector = pd.read_csv("data/processed/b3-sectors.csv")

df = df.merge(df_sector, how="left", on="CODIGO")

df["DENOM_COMERC"] = df["DENOM_COMERC"].fillna(df["DENOM_SOCIAL"])

df = df.rename(columns={"DENOM_COMERC": "NOME"})

df = df.drop(["CODIGO", "DENOM_SOCIAL"], axis=1)

df = df.sort_values(by=["SETOR", "SUBSETOR", "SEGMENTO", "CD_CVM"])

df.columns = [
    "NAME",
    "CD_CVM",
    "NUM_COMMON",
    "NUM_PREFERENTIAL",
    "NUM_TOTAL",
    "FOUNDATION",
    "AVAILABLE_COMMON",
    "AVAILABLE_PREFERENTIAL",
    "AVAILABLE_TOTAL",
    "WEB_PAGE",
    "TICKERS",
    "GOVERNANCE_LEVEL",
    "SECTOR",
    "SUBSECTOR",
    "SEGMENT",
]

print(df)

df.to_csv("data/processed/stocks-basic-info.csv", index=False)
