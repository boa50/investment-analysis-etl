import pandas as pd


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
            create_ticker_df(19348, "ITUB4;ITUB3", 4958290359, 4845844989),
            create_ticker_df(2453, "CMIG4;CMIG3", 956601911, 1905179984),
            create_ticker_df(18660, "CPFE3", 1152254440, 0),
            create_ticker_df(19445, "CSMG3", 380253069, 0),
            create_ticker_df(18627, "SAPR3;SAPR4;SAPR11", 503735259, 1007470260),
            create_ticker_df(14443, "SBSP3", 683509869, 0),
        ]
    )

    df = df.reset_index(drop=True)

    return df


df = pd.read_csv("data/raw/cad_cia_aberta.csv", encoding="ISO-8859-1", sep=";")

df = df[(df["SIT"] == "ATIVO")]

columns = ["CD_CVM", "DENOM_COMERC"]

cd_cvm_test = [
    1023,  # Banco do Brasil
    906,  # Bradesco
    20257,  # Taesa
    18376,  # Transmissões Paulista
    19348,  # Itaú
    # 922,  # Banco da Amazônia
    # 22616,  # Banco BTG
    # 20532,  # Santander
    2453,  # CEMIG
    18660,  # CPFL Energias
    # 2437,  # Eletrobras
    # 20010,  # Equatorial Energia
    # 17329,  # ENGIE
    # 15539,  # NeoEnergia
    19445,  # COPASA
    18627,  # SANEPAR
    14443,  # SABESP
]

df = df[df["CD_CVM"].isin(cd_cvm_test)][columns]
df = df.drop_duplicates()

ticker_df = get_all_stock_info()

df = pd.merge(df, ticker_df, on="CD_CVM")

### Joining with sector table
df["CODIGO"] = df["TICKERS"].str[:4]

df_sector = pd.read_csv("data/processed/b3-sectors.csv")

df = df.merge(df_sector, how="left", on="CODIGO")

df = df.drop("CODIGO", axis=1)

print(df.head())

df.to_csv("data/processed/stocks-basic-info.csv", index=False)
