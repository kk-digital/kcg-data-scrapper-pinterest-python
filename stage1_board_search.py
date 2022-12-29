import os
import sqlite3
import sys
import time
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service as chrome_service
from selenium.webdriver.chrome.options import Options as chrome_options
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

from sel import Sel

# creating the output folder 
out_folder = 'outputs'
os.makedirs(out_folder, exist_ok=True)

file_out_path = os.path.join(out_folder, 'output_of_first_tool.csv')
Separator_for_csv = "\t"
DATABASE_PATH = 'database.db'
TIME_TIME_WAIT_UNTIL_THE_WEB_LOADED = 0.5
all_data = {}

def delete_all_data_in_database():
    delete_database()
    create_database()


def create_database():
    cmd1 = '''CREATE TABLE stage1 (
    search_term TEXT    NOT NULL,
    board_url   TEXT    PRIMARY KEY,
    scraped     INTEGER DEFAULT (0) 
    );
    '''
    cmd2 = '''CREATE TABLE stage2 (
    board_url  TEXT,
    pin_url    TEXT    PRIMARY KEY,
    downloaded INTEGER DEFAULT (0) 
    );
    '''
    cmd3 = '''CREATE TABLE image_url (
    url        TEXT    NOT NULL
                       PRIMARY KEY,
    downloaded INTEGER DEFAULT (0) 
    );'''

    cmd4 = '''CREATE TABLE report (
    id        INTEGER    PRIMARY KEY
                       AUTOINCREMENT,
    total_images INTEGER NOT NULL DEFAULT(0) 
    );'''

    cmd5 = '''CREATE TABLE failed_pin_links (
    pin_url TEXT PRIMARY KEY NOT NULL,
    error TEXT NOT NULL
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
    c.execute(cmd4)
    db.commit()
    c.execute(cmd5)
    db.commit()
    

def delete_database():
    try:
        if(os.path.exists(DATABASE_PATH)):
            os.remove(DATABASE_PATH)
    except Exception as e:
        print(str(e))
        time.sleep(1)
        delete_database()


def insert_data_into_database(arg1, arg2):
    try:
        cmd = "insert into stage1(search_term, board_url) values ('" + \
            arg1.replace("'", "''")+"', '"+arg2+"')"
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.execute(cmd)
            conn.commit()
    except Exception as e:
        if(str(e).find('lock') != -1 or str(e).find('attempt to write a readonly database') != -1):
            time.sleep(1)

def scrape_board_urls(driver,search_term,args):
    print(f"Starting scraping boards for {search_term}")
    driver.set_page_load_timeout(3000)
    driver.get(
        "https://www.pinterest.com/search/boards/?q="+search_term.replace(" ", "%20")+"&rs=filter")
    driver.execute_script("document.body.style.zoom='50%'")
    main_container_div = driver.find_element(By.XPATH,"/html[1]/body[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]")
    similar_board_urls = 1
    stop_scrap = False
    while(not stop_scrap):
        board_link_elements = main_container_div.find_elements(By.TAG_NAME,"a")
        for board_link_element in board_link_elements:
            board_url = board_link_element.get_attribute('href')
            if(board_url not in all_data.keys()):
                board_info = get_board_data(board_link_element)
                all_data[board_url] = [search_term, board_info["image_count"], board_info["board_name"]]
                print(f"Board {board_info['board_name']} has {board_info['image_count']} pins, \n link: {board_url}")
            else:
                similar_board_urls += 1
            if similar_board_urls >= len(board_link_elements) or len(all_data.keys()) == args["board_limit"]:
                stop_scrap = True
                break
        driver.execute_script("arguments[0].scrollIntoView(true);",board_link_elements[-1])
        time.sleep(1)
        similar_board_urls = 1

def get_board_data(board_element):
    returns = {"board_name":"","image_count":0}
    divs = board_element.find_elements(By.TAG_NAME,"div")
    for div in divs:
        if div.get_attribute("data-test-id") == "board-card-title":
            returns["board_name"] = div.text
        elif div.get_attribute("style") == "-webkit-line-clamp: 1;":
            div_text = div.text.replace("\n","")
            returns["image_count"] = div_text.split(" ")[0].replace(",","")
    return returns

class Stage1: 
    def __init__(self,args) -> None:
        self.args = args
    
    def run(self, search_term: str) -> None: 
        """
            function that runs the first stage of the scraping given the search term. 
            The first stage is given a search term it searches for boards with the `search_term` and stores their 
            urls in a sqlite DB to be used in collecting the pins urls in stage 2.
        """
        # if(sys.argv[1] == '-o'):
        #     print('You forgot the search term.')
        #     exit()
        option = 'y'
        print("This program will delete all data in database!")
        if(option == 'y'):
            delete_all_data_in_database()
        else:
            exit()
        sel = Sel(self.args)
        driver = sel.get_driver()
        search_term = search_term
        scrape_board_urls(driver, search_term,self.args)
        driver.close()
        driver.quit()
        
        # output
        with open(file_out_path, "w", encoding='utf8') as f:
            for url in all_data:
                data = all_data[url]
                f.write(str(data[0]))
                f.write(Separator_for_csv)
                f.write(str(url))
                f.write(Separator_for_csv)
                f.write(str(data[1]))
                f.write(Separator_for_csv)
                f.write(str(data[2]))
                f.write("\n")
                insert_data_into_database(search_term, str(url))
        print(f"Finished scraping boards, found {len(all_data.keys())} boards.")

if __name__ == '__main__':
    stage1 = Stage1() 
    stage1.run("bears")