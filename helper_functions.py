from consts import DATABASE_PATH
from selenium.webdriver.chrome.service import Service as chrome_service
from selenium.webdriver.chrome.options import Options as chrome_options
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import sqlite3
import os 
import time
from selenium.webdriver.common.by import By
import requests


def init_driver():
    """ Creating driver object """
    options = chrome_options()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    prefs = {
        "profile.managed_default_content_settings.images": 2
    }
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(service=chrome_service(
        ChromeDriverManager().install()), options=options)


def save_html_page(url, filename):
    """Save html page to html erros folders"""
    
    folder_name = 'html_errors'
    os.makedirs(folder_name, exist_ok=True)
    
    response = requests.get(url)
    with open(os.path.join(folder_name, filename), 'w') as file:
        file.write(response.text)

def create_database():
    """Creates the DB"""
    # stage 1 table structure 
    cmd1 = '''CREATE TABLE stage1 (
    search_term TEXT    NOT NULL,
    board_url   TEXT    NOT NULL,
    pin_count INTEGER DEFAULT (0),
    sections_count INTEGER DEFAULT (0)
    );
    '''
    # # stage 1 table structure 
    # cmd1 = '''CREATE TABLE stage1 (
    # search_term TEXT    NOT NULL,
    # board_url   TEXT    PRIMARY KEY,
    # pin_count INTEGER DEFAULT (0)
    # );
    # '''

    # # stage 2 table structure 
    # cmd2 = '''CREATE TABLE stage2 (
    # board_url  TEXT,
    # pin_url    TEXT    PRIMARY KEY,
    # downloaded INTEGER DEFAULT (0) 
    # );
    # '''
    # stage 2 table structure 
    cmd2 = '''CREATE TABLE stage2 (
    board_url  TEXT,
    pin_url    TEXT
    );
    '''
    # # image_url table structure
    # cmd3 = '''CREATE TABLE image_url (
    # url        TEXT    NOT NULL
    #                    PRIMARY KEY,
    # downloaded INTEGER DEFAULT (0) 
    # );'''
    # image_url table structure
    cmd3 = '''CREATE TABLE image_url (
    pin_url        TEXT    NOT NULL PRIMARY KEY,
    img_url        TEXT    NOT NULL
    );'''

    db = sqlite3.connect(DATABASE_PATH)
    c = db.cursor()
    c.execute('PRAGMA encoding="UTF-8";')
    c.execute(cmd1)
    db.commit()
    c.execute(cmd2)
    db.commit()
    c.execute(cmd3)
    db.commit()


def delete_database():
    """ deletes the database"""
    if(os.path.exists(DATABASE_PATH)):
        os.remove(DATABASE_PATH)


def get_page_hash(driver):
    '''
    Returns html dom hash
    '''
    # can find element by either 'html' tag or by the html 'root' id
    dom = driver.find_element(By.TAG_NAME,'html').get_attribute('innerHTML')
    return hash(dom.encode('utf-8'))



def page_has_loaded(driver, sleep_time = 1):
    '''
    Waits for page to completely load by comparing current page hash values.
    '''

    page_hash = 'empty'
    page_hash_new = ''
    
    # comparing old and new page DOM hash together to verify the page is fully loaded
    while page_hash != page_hash_new: 
        page_hash = get_page_hash(driver)
        time.sleep(sleep_time)
        page_hash_new = get_page_hash(driver)
        #print('<page_has_loaded> - page not loaded')

        
    print("[INFO] page loaded.")
