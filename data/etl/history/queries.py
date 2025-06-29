import data.db as db

db.get_db_credentials()


def get_basic_info():
    sql = f"""
            SELECT CD_CVM, NUM_TOTAL
            FROM {db.get_table_full_name("stocks-basic-info")}
        """

    return db.execute_query(sql)


def get_stocks_available():
    sql = f"""
            SELECT CD_CVM, TICKER
            FROM {db.get_table_full_name("stocks-available")}
        """

    return db.execute_query(sql)


def get_prices():
    sql = f"""
            SELECT DATE, TICKER, PRICE
            FROM {db.get_table_full_name("stocks-prices")}
        """

    return db.execute_query(sql)


def get_dividends():
    sql = f"""
            SELECT DATE, TICKER, VALUE
            FROM {db.get_table_full_name("vw-stocks-dividends")}
        """

    return db.execute_query(sql)


def get_fundaments():
    sql = f"""
            SELECT CD_CVM, DT_END, KPI, VALUE AS VALUE_FUNDAMENT, VALUE_ROLLING_YEAR
            FROM {db.get_table_full_name("stocks-fundaments")}
        """

    return db.execute_query(sql)
