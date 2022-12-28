import json
import sqlite3
import sys
import time
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service as chrome_service
from selenium.webdriver.chrome.options import Options as chrome_options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import time
import re
import os 
from helper_functions import page_has_loaded , init_driver


# creating output folder.
out_folder = 'outputs'
os.makedirs(out_folder, exist_ok=True)

file_out_path = os.path.join(out_folder, 'output_of_second_tool.json')
Separator_for_csv = "\t"
DATABASE_PATH = "database.db"
HOW_MANY_WINDOWS_DO_YOU_NEED = 1


class window:
    all_links = {}
    count_load_failt = 0
    temp_length = -1
    count = 0
    board_url= None
    ending_count = 0

    def __init__(self, driver, window_handle):
        self.driver = driver
        self.window_name = window_handle

    def load_board_page(self, board_url):
        #print("[INFO] IN LOAD BOARD PAGE")
        self.all_links = {}
        self.driver.switch_to.window(self.window_name)
        self.board_url = board_url
        self.driver.delete_all_cookies()
        try:
            self.driver.get(self.board_url)
            self.count_load_failt = 0
            #print(f"[INFO] Went to {self.board_url}")
        except:
            #print("[INFO] FAILED IN LOAD BOARD PAGE")
            self.count_load_failt += 1
            if(self.count_load_failt == 4):
                return
            else:
                self.load_board_page(board_url)

        self.driver.execute_script("document.body.style.zoom='50%'")
        print("[INFO] JS SCRIPTS EXECUTED")
        
    def is_loaded_full_images(self, first_roll):
        #time.sleep(0.3)
        #print("[INFO] IN IS LOADED FULL IMAGES")
        if(self.temp_length == len(self.all_links)):
            self.count += 1
            if(self.count == 5):
                self.count = 0
                print("[INFO] FINISHED FETCHING .. ")
                return False
        else:
            self.temp_length = len(self.all_links)
            self.count = 0

        if not first_roll:
            self.driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight-5) );")
            print("[INFO] page scrolled")
            page_has_loaded(self.driver)
        return True

    def get_link_pin(self):
        #print("[INFO] IN GET LINK PIN")
        self.driver.switch_to.window(self.window_name)
        get_url = self.driver.current_url
        #print(f"[INFO] THE CURRENT PAGE WINDOW IN IS {get_url}")
        self.driver.implicitly_wait(20) # gives an implicit wait for 20 seconds
        
        try: 
            mother_of_a_tag = self.driver.find_element(By.XPATH, "//div[@class='gridCentered']")        
        except Exception:
            mother_of_a_tag = self.driver.find_element(By.XPATH, "//div[@role='main']")

        mother_of_a_tag = mother_of_a_tag.get_attribute('innerHTML')
        soup = BeautifulSoup(mother_of_a_tag, 'html.parser')
        for i in soup.find_all('a'):
            try:
                id = i['href']
            except:
                continue
            if(id.find("/pin/") != -1):
                self.all_links[id] = None
                self.push_to_database("https://www.pinterest.com"+id)
            
    def push_to_database(self, pin_url):
        
        cmd = "insert into stage2(board_url, pin_url) values ('" + \
            str(self.board_url)+"','"+str(pin_url)+"')"
        
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            if(str(e).lower().find("unique") != -1):
                pass
            elif(str(e).lower().find("database is locked") != -1):
                time.sleep(1)
                self.push_to_database(pin_url)
            else:
                print(str(e))


def process(board_list):
    driver = init_driver()
    driver.set_window_size(1280,720)
    windows = [window(driver, i) for i in driver.window_handles]
    
    for index , board in enumerate(board_list):
        board_url = f"https://www.pinterest.com{board}"
        windows[0].load_board_page(board_url)
        windows[0].get_link_pin()

        first_roll = True
        while(windows[0].is_loaded_full_images(first_roll)): # waiting for the page to scroll and then collect pins
            windows[0].get_link_pin()
            links_count = len(windows[0].all_links)
            print(f"[INFO] {links_count} pins scrapped in board {board_url}")
            first_roll = False
        #set_board_is_scraped(board_url)
        print(f"[INFO] FINISHED URL:{board_url}; PINS = {links_count}")

def count_pins_in_board(board_url):
    pins_count = None
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.execute("SELECT count(*) as pins_count from stage2 WHERE board_url=?",(board_url,)).fetchall()
            conn.commit()
            pins_count = cursor[0][0]

    except Exception as e :
        print(f"[ERROR] cannot count pins in stage2 in board {board_url}!, because of {e}")
        time.sleep(1)
        return count_pins_in_board(board_url)
    return pins_count


def board_pins_count_report(search_term):
    boards = get_board_urls(search_term)
    
    for board in boards:
        board_url = f"https://www.pinterest.com{board}"
        scrapped_pins_count = count_pins_in_board(board_url)
        true_pins_count     = get_true_pins_count(board)
        print(f"[INFO] BOARD {board} , SCRAPPED {scrapped_pins_count} , TRUE {true_pins_count}")


def get_true_pins_count(board):
    true_pins_count = None
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.execute("SELECT pin_count FROM stage1 WHERE board_url=?",(board,)).fetchone()
            conn.commit()
            true_pins_count = cursor[0]

    except Exception as e :
        print(f"[ERROR] cannot get true pins count!, because of {e}")
        time.sleep(1)
        return get_true_pins_count(board)
    return true_pins_count



def get_board_urls(search_term):
    returns = []
    #cmd = "select board_url from stage1"
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.execute("SELECT board_url from stage1 WHERE search_term=?",(search_term,))
            conn.commit()
            for i in cursor:
                returns.append(i[0])

    except Exception as e :
        print(f"[ERROR] cannot get the boards urls!, because of {e}")
        time.sleep(1)
        return get_board_urls(search_term)
    return returns

# def get_search_term():
#     cmd = "select search_term from stage1 LIMIT 1;"
#     try:
#         with sqlite3.connect(DATABASE_PATH) as conn:
#             cursor = conn.execute(cmd)
#             conn.commit()
#             for i in cursor:
#                 return i[0]
#     except:
#         time.sleep(1)
#         return get_search_term()



class Stage2: 
    def __init__(self,search_term) -> None:
        self.search_term = search_term
    
    def run(self) -> None: 
        """
            function that executes the second stage of Pintrest scraping, which is retrieveing the board urls which was collected 
            in stage 1 and goes in them 1 by one to collect all pens urls within those boards and storing the pin urls inside the sqlite DB. 
        """
        
        board_urls = get_board_urls(self.search_term)
    
        process(board_urls)

        board_pins_count_report(self.search_term)

        #output_json_file()
 
        return 


# def set_board_is_scraped(url):
#     cmd = "update stage1 set scraped = 1 where board_url = '"+url+"';"
#     try:
#         with sqlite3.connect(DATABASE_PATH) as conn:
#             conn.execute(cmd)
#             conn.commit()
#     except Exception as e:
#         print(str(e))
#         time.sleep(1)
#         return set_board_is_scraped(url)
    
        
# def output_json_file():
#     json_data = []
#     data = {}
#     number_of_images = {}
#     cmd = "select board_url, pin_url from stage2"
#     with sqlite3.connect(DATABASE_PATH) as conn:
#         cursor = conn.execute(cmd)
#         conn.commit()
#     cursor_temp = []
#     for i in cursor:
#         cursor_temp.append(i)
#     cmd = "select board_url, pin_url, count(pin_url) from stage2 group by board_url"
#     with sqlite3.connect(DATABASE_PATH) as conn:
#         cursor_for_pins = conn.execute(cmd)
#         conn.commit()
#     for cur in cursor_for_pins:
#         number_of_images[cur[0]] = cur[2]
#     for cur in cursor_temp:
#         data[cur[0]] = None
    
#     count = 0
#     for board_url in data:
#         count+=1
#         print("Writting ", count," -> ", board_url)
#         pins = []
#         for cur in cursor_temp:
#             if(cur[0] == board_url):
#                 pins.append(cur[1])
#         # print({"board url": board_url, "number of images": number_of_images[board_url], "pins": pins})
#         json_data.append({"board url": board_url, "number of images": number_of_images[board_url], "pins": pins})
#     json_data = {get_search_term():json_data}
#     json_string = json.dumps(json_data)

#     with open(file_out_path, 'w') as outfile:
#         outfile.write(json_string)

# if __name__ == '__main__':
#     stage2 = Stage2()
#     stage2.run()
    