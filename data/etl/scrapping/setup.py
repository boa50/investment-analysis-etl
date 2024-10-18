from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait


def setup_selenium(url, is_headless=True):
    options = webdriver.FirefoxOptions()
    if is_headless:
        options.add_argument("-headless")

    driver = webdriver.Firefox(
        service=FirefoxService(GeckoDriverManager().install()), options=options
    )
    driver.get(url)

    wait = WebDriverWait(driver, 3)

    return driver, wait
