import data.db as db

db.get_db_credentials()


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


def delete_data_from_fundaments(years_to_delete: list):
    sql = f"""
            DELETE
            FROM {db.get_table_full_name("stocks-fundaments")}
            WHERE DT_YEAR IN ({",".join([str(year) for year in years_to_delete])})
        """

    db.execute_query(sql)


def delete_data_from_history(first_year_to_delete: int):
    sql = f"""
            DELETE
            FROM {db.get_table_full_name("stocks-history")}
            WHERE DATE >= '{first_year_to_delete}-01-01'
        """

    db.execute_query(sql)


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


def get_all_files_download_control():
    sql = f"""
            SELECT NAME
            FROM {db.get_table_full_name("files-download-control")}
        """

    return db.execute_query(sql)


def get_files_last_download_date(file_data: str):
    sql = f"""
            SELECT *
            FROM {db.get_table_full_name("files-download-control")}
            WHERE NAME LIKE '{file_data}_cia_aberta_%'
        """

    return db.execute_query(sql)


def insert_on_control_table(filename: str, date: str):
    sql = f"""
            INSERT INTO {db.get_table_full_name("files-download-control")}
            VALUES ('{filename}', '{date}')
        """

    return db.execute_query(sql)


def update_control_table(filename: str, date: str):
    sql = f"""
            UPDATE {db.get_table_full_name("files-download-control")}
            SET DATE = '{date}'
            WHERE NAME = '{filename}'
        """

    return db.execute_query(sql)


def get_available_stocks():
    sql = f"""
            SELECT *
            FROM {db.get_table_full_name("stocks-available")}
        """

    return db.execute_query(sql)
