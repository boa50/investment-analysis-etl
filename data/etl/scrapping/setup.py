from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import requests
import random


def scrap_proxies():
    proxy_list_url = "https://www.sslproxies.org/"

    req = requests.get(proxy_list_url)

    soup = BeautifulSoup(req.text, "html.parser")

    cells = soup.find("table").find_all("td")

    n_cells_per_row = 8

    df_proxies = pd.DataFrame()

    for row_idx in range(int(len(cells) / n_cells_per_row)):
        row_start = row_idx * n_cells_per_row
        row_end = row_start + n_cells_per_row
        row = cells[row_start:row_end]

        proxy = pd.DataFrame(
            {
                "PROXY": f"{row[0].text}:{row[1].text}",
            },
            index=[df_proxies.shape[0]],
        )

        df_proxies = pd.concat([df_proxies, proxy])

        if df_proxies.shape[0] >= 20:
            break

    df_proxies.to_csv("data/raw/_proxies.csv", index=False)

    return df_proxies


def get_proxy():
    try:
        df_proxies = pd.read_csv("data/raw/_proxies.csv")
    except FileNotFoundError:
        df_proxies = scrap_proxies()

    return random.choice(df_proxies["PROXY"].values)


def setup_selenium(url, is_headless=True, proxy=get_proxy()):
    timeout = 3
    options = webdriver.FirefoxOptions()

    options.proxy = Proxy(
        {
            "proxyType": ProxyType.MANUAL,
            "httpProxy": proxy,
            "sslProxy": proxy,
        }
    )

    if is_headless:
        options.add_argument("-headless")

    driver = webdriver.Firefox(
        service=FirefoxService(GeckoDriverManager().install()), options=options
    )

    driver.set_page_load_timeout(timeout)

    try:
        driver.get(url)
    except Exception as error:
        print(error)
        driver.quit()
        setup_selenium(url, is_headless=is_headless, proxy=get_proxy())

    wait = WebDriverWait(driver, timeout)

    return driver, wait


url = "https://api.ipify.org/"

driver, wait = setup_selenium(url, is_headless=False)
