from bs4 import BeautifulSoup
import time

import pyotp
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.service import Service

from selenium import webdriver

from selenium.common.exceptions import WebDriverException

def get_html(user_id, password, url, secret):

    remote_webdriver = 'remote_chromedriver'

    # begin service and build driver
    # service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options)

    # initially open page, prompting a login screen
    driver.get(url)
    # wait, at least until element is successfully found (see implicitly_wait docu)
    driver.implicitly_wait(12)

    # login using username, password
    driver.find_element(by="id", value="email").send_keys(user_id)
    driver.find_element(by="id", value="password").send_keys(password)
    # click login button
    driver.find_element(by="name", value="commit").click()

    # this is just to ensure that the page is loaded
    time.sleep(12)

    #wait for the 2FA feild to display
    authField = WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.ID, "code")))

    # get the token from google authenticator
    totp = pyotp.totp.TOTP(secret)
    token = totp.now()
    print (token)
    # enter the token in the UI
    authField.send_keys(token)
    # click on the button to complete 2FA
    driver.find_element(by="name", value="commit").click()

    # this is just to ensure that the page is loaded
    time.sleep(20)

    # now can grab page info that page has been loaded and dynamic info as well
    page = driver.page_source

    # this renders the JS code and stores all information in static HTML code.

    # Now, we could simply apply bs4 to html variable
    soup = BeautifulSoup(page, "html.parser")
    # print(soup.prettify())

    driver.quit()  # closing the webdriver

    return soup


def scrape(page):
    list_cards = page.find_all("div", attrs={"class": "list-item--index-card d-f"})

    list_counts = {}

    for card in list_cards:

        l_name = card.find("span", attrs={"class": None}).text
        # print(l_name)

        l_count = int(card.find("div", attrs={"class": "count badge badge--subtle"}).text)
        # print(l_count)

        if "Youth" in l_name:
            list_counts[l_name] = l_count
            # print(list_counts)

    return list_counts


def get_credentials():
    with open('/opt/airflow/Documents/pcp_user.txt', 'r') as a, \
            open('/opt/airflow/Documents/pcp_pw.txt', 'r') as b:
        user_id = a.read()
        password = b.read()

        return user_id, password
    

def get_secret():
     with open('/opt/airflow/Documents/google_auth_sec.txt', 'r') as a:
        
        secret = a.read()

        return secret


def validate(web_list_count, pco_name, pco_count):

    for name, count in web_list_count.items():
        if name == pco_name:
            # print(f"{pco_name} --> web count is: {count}. pco count is: {pco_count}")
            if web_list_count[name] == pco_count:
                return 1
            else:
                return 0

