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




def wait_until_load_full_images(driver, search_term):
    temp = -1
    warnning = -1
    #driver.maximize_window() # For maximizing window
    driver.implicitly_wait(20) # gives an implicit wait for 20 seconds
    while(1):
        time.sleep(1)
        tag_div = driver.find_element(By.XPATH , "//div[@role='list']")
        get_boards(tag_div.get_attribute('innerHTML'), search_term)
        print(f"\n\n\nboard count: {len(all_data)}\n\n\n")
        if(len(all_data) == temp):
            warnning += 1
            if(warnning == 10):
                break
        else:
            temp = len(all_data)
            warnning = 0


def find_comma(str):
    for i in range(len(str)):
        if(str[i] == ','):
            try:
                int(str[i+1])
                return i
            except:
                try:
                    int(str[i+2])
                    return i+1
                except:
                    continue
    return -1


def get_image_count(soup_a):
    re = ''
    for i in soup_a.find_all("div"):
        try:
            if(i['style'] == '-webkit-line-clamp: 1;'):
                re += i.text
        except:
            pass
    re = re.replace('\n', '')
    if(re[find_comma(re)+1:re.find("Pins")] == ''):
        return re
    return re[find_comma(re)+1:re.find("Pins")]


def get_board_name(soup_a):
    for i in soup_a.find_all("div"):
        try:
            if(i['title'] != ''):
                return i.text
        except:
            pass


def get_boards(html, search_term):
    global all_data
    soup = BeautifulSoup(html, 'html.parser')
    
    for a in soup.find_all("a"):
        # print(a.prettify())
        url = a["href"]
        print(url)
        image_count = get_image_count(a)
        board_name = get_board_name(a)
        # print([url, image_count, board_name])
        all_data[url] = [search_term, image_count, board_name]


def first_tool(driver, search_term):
    driver.delete_all_cookies()
    driver.get(
        "https://www.pinterest.com/search/boards/?q="+search_term.replace(" ", "%20")+"&rs=filter")

    driver.execute_script("document.body.style.zoom='50%'")
    driver.maximize_window()
    driver.execute_script(
        "javascript:setInterval(function(){window.scrollBy(0, window.innerHeight);}, Math.floor(200));")
    print("Watting for load all images...")
    wait_until_load_full_images(driver, search_term)
    print("All images is loaded.")
    return


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
        first_tool(driver, search_term)
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
        print("finished stage 1")   

if __name__ == '__main__':
    stage1 = Stage1() 
    stage1.run("bears")