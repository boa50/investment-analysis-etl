import data.db as db


def get_ipca():
    sql = f"""
            SELECT *
            FROM {db.get_table_full_name("ipca")}
        """

    return db.execute_query(sql)


def get_history(n_years=10):
    table_full_name = db.get_table_full_name("stocks-history")

    sql = f"""
            SELECT *
            FROM {table_full_name}
            WHERE DATE >= DATE_SUB((SELECT max(DATE) FROM {table_full_name}), INTERVAL {n_years} YEAR)
        """

    return db.execute_query(sql)


def get_fundaments(n_years=10):
    table_full_name = db.get_table_full_name("stocks-fundaments")

    sql = f"""
            SELECT CD_CVM, DT_END as DATE, KPI, VALUE, VALUE_ROLLING_YEAR
            FROM {table_full_name}
            WHERE DT_YEAR >= (SELECT max(DT_YEAR) FROM {table_full_name}) - {n_years}
        """

    return db.execute_query(sql)


def get_segments():
    sql = f"""
            SELECT CD_CVM, TICKERS, SEGMENT
            FROM {db.get_table_full_name("stocks-basic-info")} 
        """

    df_segments = db.execute_query(sql)

    df_segments["TICKERS"] = df_segments["TICKERS"].str.split(";")
    df_segments = df_segments.explode("TICKERS")
    df_segments = df_segments.rename(columns={"TICKERS": "TICKER"})

    return df_segments.reset_index(drop=True)
