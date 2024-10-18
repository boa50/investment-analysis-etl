from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup

from setup import setup_selenium

import pandas as pd


def load_dividends_from_page(html):
    df_dividends = pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")

    cells = soup.find(id="accordionMoney").find_all("td")

    n_cells_per_row = 10

    for row_idx in range(int(len(cells) / n_cells_per_row)):
        row_start = row_idx * n_cells_per_row
        row_end = row_start + n_cells_per_row
        row = cells[row_start:row_end]

        dividend = pd.DataFrame(
            {
                "DATE": row[5].text,
                "VALUE": float(row[2].text.replace(",", ".")),
                "TYPE": row[4].text,
            },
            index=[df_dividends.shape[0]],
        )

        df_dividends = pd.concat([df_dividends, dividend])

    return df_dividends


def load_dividends(cd_cvm, ticker_base_code):
    url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/{}/{}/corporate-actions?language=pt-br".format(
        cd_cvm, ticker_base_code
    )

    driver, wait = setup_selenium(url, is_headless=True)

    select_dividends = Select(
        wait.until(EC.element_to_be_clickable((By.ID, "selectType")))
    )
    select_dividends.select_by_value("2")

    try:
        select_elements_per_page = Select(
            wait.until(EC.element_to_be_clickable((By.ID, "selectPage")))
        )
        select_elements_per_page.select_by_index(2)
    except:
        print("There is less then 60 dividend registers!!!")

    try:
        list_pagination = wait.until(
            EC.element_to_be_clickable((By.ID, "listing_pagination"))
        )
        list_pages = list_pagination.find_elements(by=By.CSS_SELECTOR, value="a")
        next_page = list_pages[-1]
    except:
        print("There is no pagination!!!")

    df_dividends = pd.DataFrame()

    df_new_dividends = load_dividends_from_page(driver.page_source)
    df_dividends = pd.concat([df_dividends, df_new_dividends])

    try:
        next_page.click()

        df_new_dividends = load_dividends_from_page(driver.page_source)
        df_dividends = pd.concat([df_dividends, df_new_dividends])
    except:
        print("There isn't another page to load")

    try:
        next_page.click()

        df_new_dividends = load_dividends_from_page(driver.page_source)
        df_dividends = pd.concat([df_dividends, df_new_dividends])
    except:
        print("There isn't another page to load")

    df_dividends["DATE"] = pd.to_datetime(df_dividends["DATE"], format="%d/%m/%Y")

    return df_dividends


print(load_dividends(1023, "BBAS"))
