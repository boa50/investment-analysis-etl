from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

from bs4 import BeautifulSoup

options = webdriver.FirefoxOptions()
options.add_argument("-headless")
options.add_argument("window-size=1920,1080")
options.add_argument("start-maximized")

driver = webdriver.Firefox(
    service=FirefoxService(GeckoDriverManager().install()), options=options
)
driver.get("https://ri.taesa.com.br/servicos-aos-investidores/central-de-downloads/")

wait = WebDriverWait(driver, 1)

# files_filter = wait.until(EC.element_to_be_clickable((By.NAME, "slug_pt")))

# ActionChains(driver).move_to_element(files_filter).click().perform()

# files_filter.click()

# reject_cookies = wait.until(
#     EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
# )
# ActionChains(driver).move_to_element(reject_cookies).click().perform()

# select = Select(driver.find_element(By.NAME, "slug_pt"))
select = Select(wait.until(EC.element_to_be_clickable((By.NAME, "slug_pt"))))

select.select_by_value("tabela-auxiliar")

# files_filter = driver.find_element(
#     By.NAME,
#     "slug_pt",
# )

first_file = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "carregado")))

html = driver.page_source

driver.quit()

soup = BeautifulSoup(html, "html.parser")

files = soup.find_all("div", class_="carregado")

for file in files:
    print(file)
