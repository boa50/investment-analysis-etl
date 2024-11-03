import pandas as pd


### Doing it manually to avoid making more requests
def get_all_stock_info():
    def create_ticker_df(
        cd_cvm, tickers, num_ordinarias, num_preferenciais, foundation
    ):
        return pd.DataFrame(
            {
                "CD_CVM": cd_cvm,
                "TICKERS": tickers,
                "NUM_ORDINARIAS": num_ordinarias,
                "NUM_PREFERENCIAIS": num_preferenciais,
                "NUM_TOTAL": num_ordinarias + num_preferenciais,
                "FOUNDATION": foundation,
            },
            index=[0],
        )

    df = pd.concat(
        [
            # Data obtained from https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/1023/overview/overview?language=pt-br
            create_ticker_df(906, "BBDC3;BBDC4", 5330304681, 5311865547, 1943),
            create_ticker_df(1023, "BBAS11;BBAS12;BBAS3", 5730834040, 0, 1808),
            create_ticker_df(18376, "TRPL3;TRPL4", 257937732, 400945572, 1999),
            create_ticker_df(20257, "TAEE11;TAEE3;TAEE4", 590714069, 442782652, 2006),
            create_ticker_df(19348, "ITUB4;ITUB3", 4958290359, 4845844989, 1943),
            create_ticker_df(2453, "CMIG4;CMIG3", 956601911, 1905179984, 1952),
            create_ticker_df(18660, "CPFE3", 1152254440, 0, 1998),
            create_ticker_df(19445, "CSMG3", 380253069, 0, 1963),
            create_ticker_df(18627, "SAPR3;SAPR4;SAPR11", 503735259, 1007470260, 1963),
            create_ticker_df(14443, "SBSP3", 683509869, 0, 1973),
            create_ticker_df(922, "BAZA3", 56058315, 0, 1942),
            create_ticker_df(22616, "BPAC3;BPAC5;BPAC11", 7244165568, 4261954360, 1983),
            create_ticker_df(20532, "SANB3;SANB4;SANB11", 3818695031, 3679836020, 1982),
            create_ticker_df(2437, "ELET3;ELET5;ELET6", 2027011498, 280088314, 1962),
            create_ticker_df(20010, "EQTL3", 1248075298, 0, 1999),
            create_ticker_df(17329, "EGIE3", 815927740, 0, 1998),
            create_ticker_df(15539, "NEOE3", 1213797248, 0, 1997),
            create_ticker_df(24961, "AMBP3", 167041869, 0, 1995),
            create_ticker_df(16861, "CASN3;CASN4", 572078479, 578578081, 1970),
            create_ticker_df(25550, "ORVR3", 82950889, 0, 2009),
        ]
    )

    df = df.reset_index(drop=True)

    return df


df = pd.read_csv("data/raw/cad_cia_aberta.csv", encoding="ISO-8859-1", sep=";")

df = df[(df["SIT"] == "ATIVO")]

ticker_df = get_all_stock_info()

cds_cvm = ticker_df["CD_CVM"].values

df = df[df["CD_CVM"].isin(cds_cvm)][["CD_CVM", "DENOM_COMERC"]]
df = df.drop_duplicates()

df = pd.merge(df, ticker_df, on="CD_CVM")

### Joining with sector table
df["CODIGO"] = df["TICKERS"].str[:4]

df_sector = pd.read_csv("data/processed/b3-sectors.csv")

df = df.merge(df_sector, how="left", on="CODIGO")

df = df.drop("CODIGO", axis=1)

df = df.sort_values(by=["SETOR", "SUBSETOR", "SEGMENTO", "CD_CVM"])

print(df)

df.to_csv("data/processed/stocks-basic-info.csv", index=False)
