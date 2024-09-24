import os
from bs4 import BeautifulSoup
import time
import pyotp
import logging
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_html(user_id, password, url, secret):
    remote_webdriver = 'remote_chromedriver'

    # Configure Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')

    try:
        # Initialize the WebDriver
        driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options)

        # Open the login page
        driver.get(url)
        driver.implicitly_wait(12)

        # Perform login
        driver.find_element(by="id", value="email").send_keys(user_id)
        driver.find_element(by="id", value="password").send_keys(password)
        driver.find_element(by="name", value="commit").click()

        # Wait for the 2FA field to display
        authField = WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.ID, "code")))

        # Generate and enter the 2FA token
        totp = pyotp.TOTP(secret)
        token = totp.now()
        logging.info(f"Generated 2FA token: {token}")
        authField.send_keys(token)
        driver.find_element(by="name", value="commit").click()

        # Wait for the page to load
        time.sleep(20)

        # Get the page source
        page = driver.page_source
        soup = BeautifulSoup(page, "html.parser")

    except (WebDriverException, TimeoutException) as e:
        logging.error(f"An error occurred: {e}")
        soup = None
    finally:
        driver.quit()  # Ensure the WebDriver is closed

    return soup

def scrape(page):
    list_cards = page.find_all("div", attrs={"class": "list-item--index-card d-f"})
    list_counts = {}

    for card in list_cards:
        l_name = card.find("span", attrs={"class": None}).text
        l_count = int(card.find("div", attrs={"class": "count badge badge--subtle"}).text)

        if "Youth" in l_name:
            list_counts[l_name] = l_count

    return list_counts

def get_credentials():
    user_id = os.getenv('PCO_USER_ID')
    password = os.getenv('PCO_PASSWORD')
    return user_id, password

def get_secret():
    secret = os.getenv('GOOGLE_AUTH_SECRET')
    return secret

def validate(web_list_count, pco_name, pco_count):
    for name, count in web_list_count.items():
        if name == pco_name:
            if web_list_count[name] == pco_count:
                return 1
            else:
                return 0
    return 0

