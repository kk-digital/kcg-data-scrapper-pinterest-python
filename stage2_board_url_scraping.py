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
from helper_functions import page_has_loaded, init_driver, create_database
import socket


# creating output folder.
out_folder = 'outputs'
os.makedirs(out_folder, exist_ok=True)

file_out_path = os.path.join(out_folder, 'output_of_second_tool.json')
Separator_for_csv = "\t"
DATABASE_PATH = "database.db"
HOW_MANY_WINDOWS_DO_YOU_NEED = 1
true_pin_count = None # true number of pins for a single board
SCROLL_IDLE_TIME = 3
POLL_TIME = 1
SCROLL_TIMEOUT = 10
TRIGGER_STOP = 5 # If your internet connection is slow, increase this number.
RESIZE_WAIT_TIME = 10
TIMEOUT = 10
THREAD_POOL_WORKERS = 5 # Size of the thread pool



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
        except Exception as e :
            #mother_of_a_tag = self.driver.find_element(By.XPATH, "//div[@role='main']")
            print(f"[ERROR] IN SEARCHING FOR XPATH, {e}")
            return

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
    
    def exist_in_db(self, pin_url):
        """checking if a board url + pin_url is in DB or not"""
        exist = 0
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT count(*) FROM stage2 WHERE board_url=? AND pin_url=?",(self.board_url,pin_url)).fetchone()
                conn.commit()
                exist = cursor[0]

        except Exception as e :
            print(f"[ERROR] cannot check board existance , because of {e}")
            time.sleep(1)
            return self.exist_in_db(pin_url)
        return exist

    def push_to_database(self, pin_url):
        if self.exist_in_db(pin_url):
            #print(f"[INFO] {pin_url} with {self.board_url} exists.")
            return 
        else:
            #print(f"[INFO] inserting {pin_url} with {self.board_url}.")
            self.insert_to_database(pin_url)

    def insert_to_database(self, pin_url):
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


def process(board_list):
    driver = init_driver()
    driver.set_window_size(1280,720)
    windows = [window(driver, i) for i in driver.window_handles]
    
    for index in range(len(board_list)):
        board = board_list[index]
        true_pin_count =  get_true_pins_count(board)
        board_url = f"https://www.pinterest.com{board}"
        windows[0].load_board_page(board_url)
        windows[0].get_link_pin()
        print(f"[INFO] STARTING PINS SCRAPING FOR BOARD {board}")

        first_roll = True
        try:
            while(windows[0].is_loaded_full_images(first_roll)): # waiting for the page to scroll and then collect pins
                try:
                    windows[0].get_link_pin()
                    links_count = len(windows[0].all_links)
                    print(f"[INFO] {links_count} pins scrapped out of {true_pin_count} in board {board_url}")
                    first_roll = False
                    windows[0].is_loaded_full_images(first_roll)
                except Exception as e:
                    print(f"[ERROR] IN PAGE LOOP, {e}")
        except Exception as e:
            print("[ERROR] BEFORE GOING TO THE PAGE LOOP, {e}")
            index -= 1
        #set_board_is_scraped(board_url)
        print(f"[INFO] FINISHED URL:{board_url}; PINS = {links_count}; TRUE NUMBER = {true_pin_count}")




class Stage2: 
    def __init__(self,search_term):
        self.search_term = search_term
        self.db_conn = None
        self.all_links = []

    def __start_connections(self):
        self.db_conn  = self.__initiate_db_conn() 
        self.driver = init_driver()


    def __initiate_db_conn(self):
        """ getting the connection object to the DB""" 
        if not os.path.exists(DATABASE_PATH):
            create_database()
        return sqlite3.connect(DATABASE_PATH)
        

    def __count_pins_in_board(self):
        pins_count = None
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT count(*) as pins_count from stage2 WHERE board_url=?",(self.board_url,)).fetchall()
                conn.commit()
                pins_count = cursor[0][0]

        except Exception as e :
            print(f"[ERROR] cannot count pins in stage2 in board {self.board_url}!, because of {e}")
            time.sleep(1)
            return self.__count_pins_in_board(self.board_url)
        return pins_count


    def __exist_in_db(self, pin_url):
        """checking if a board url + pin_url is in DB or not"""
        exist = 0
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT count(*) FROM stage2 WHERE board_url=? AND pin_url=?",(self.board_url,pin_url)).fetchone()
                conn.commit()
                exist = cursor[0]

        except Exception as e :
            print(f"[ERROR] cannot check board existance , because of {e}")
            time.sleep(1)
            return self.__exist_in_db(pin_url)
        return exist

    def __push_to_database(self, pin_url):
        if self.__exist_in_db(pin_url):
            #print(f"[INFO] {pin_url} with {self.board_url} exists.")
            return 
        else:
            #print(f"[INFO] inserting {pin_url} with {self.board_url}.")
            self.__insert_to_database(pin_url)

    def __insert_to_database(self, pin_url):
        cmd = "insert into stage2(board_url, pin_url) values ('" + \
            str(self.board_url)+"','"+str(pin_url)+"')"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
                print(f"[INFO] INSERTING {pin_url}")
        except Exception as e:
            if(str(e).lower().find("unique") != -1):
                pass
            elif(str(e).lower().find("database is locked") != -1):
                time.sleep(1)
                self.__push_to_database(pin_url)
            else:
                print(str(e))


    def __get_board_urls(self):
        returns = []
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT board_url from stage1 WHERE search_term=?",(self.search_term,))
                conn.commit()
                returns.extend(url[0] for url in cursor)
        except Exception as e :
            print(f"[ERROR] cannot get the boards urls!, because of {e}")
            time.sleep(1)
            return self.__get_board_urls()
        return returns


    def __get_link_pin(self):
        self.driver.implicitly_wait(20)
        mother_of_a_tag = self.driver.find_element(By.XPATH, "//div[@class='gridCentered']")                
        mother_of_a_tag = mother_of_a_tag.get_attribute('innerHTML')
        soup = BeautifulSoup(mother_of_a_tag, 'html.parser')
        for i in soup.find_all('a'):
            try:
                id = i['href']
            except:
                continue
            if(id.find("/pin/") != -1):
                if id not in self.all_links:
                    self.all_links.append(id)
                    self.__push_to_database("https://www.pinterest.com"+id)


    
    def __get_scroll_height(self):
        return self.driver.execute_script("return document.documentElement.scrollHeight")


    def __scroll_inner_height(self):
        self.driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight * 0.8));")
    
    def __is_netwrok_available(self):
        try:
            # Connect to a known website to check if there is an active network connection
            socket.create_connection(("www.pinterest.com", 80))
            return True
        except OSError:
            pass
        return False



    def __scroll_and_scrape(self, board):
        try:
            print(f"[INFO] STARTING SCROLLING AND SCRAPPING FOR BOARD: {board}")
            self.all_links = []
            self.driver.delete_all_cookies()
            self.driver.execute_script("document.body.style.zoom='50%'")
            self.driver.get(self.board_url)
            target_number_of_pins = self.__get_true_pins_count(board)

            scroll_trigger_count = 0
            pins_trigger_count = 0 
            while scroll_trigger_count <= TRIGGER_STOP and pins_trigger_count <= TRIGGER_STOP:

                before_pins_count = len(self.all_links)
                before_scroll_height = self.__get_scroll_height()

                print(f"[INFO] {board} SCROLLING")
                self.__scroll_inner_height()
                time.sleep(SCROLL_IDLE_TIME)
                # Scrapping the pins.
                try:
                    self.__get_link_pin()
                except Exception as e:
                    print(f"[ERROR] IN GETTING PINS; {e}")
                    pins_trigger_count = 0 
                    scroll_trigger_count = 0

                    if not self.__is_netwrok_available():
                        print("[ERROR] NETWORK ERROR; PLEASE CHECK")
                        print("[INFO] RESTARTING THE SCROLL AND SCRAPPING FUNCTION")
                        self.__scroll_and_scrape(board)
                    
                    continue

                print(f"[INFO]{board} :: NUMBER OF PINS SCRAPPED: {len(self.all_links)} PINS OUT OF {target_number_of_pins}")
                
                print(f"[INFO]{board} :: SCROLLING")           
                self.__scroll_inner_height()
                time.sleep(SCROLL_IDLE_TIME)
                # Scrapping the pins.
                try:
                    self.__get_link_pin()
                except Exception as e:
                    print(f"[ERROR] IN GETTING PINS; {e}")
                    pins_trigger_count = 0 
                    scroll_trigger_count = 0

                    if not self.__is_netwrok_available():
                        print("[ERROR] NETWORK ERROR; PLEASE CHECK")
                        print("[INFO] RESTARTING THE SCROLL AND SCRAPPING FUNCTION")
                        self.__scroll_and_scrape(board)
                                        
                    continue
                print(f"[INFO]{board} :: NUMBER OF PINS SCRAPPED: {len(self.all_links)} PINS OUT OF {target_number_of_pins}")                
                after_pins_count = len(self.all_links)

                print(f"[INFO] {board} NUMBER OF PINS SCRAPPED: {after_pins_count} PINS OUT OF {target_number_of_pins}")            
                after_scroll_height = self.__get_scroll_height()

                # Check if the scroll height is the same
                if before_scroll_height == after_scroll_height:
                    scroll_trigger_count += 1
                    print(f"[INFO] {board} SCROLL STOP TRIGGER COUNT INCREASED")
                else :
                    scroll_trigger_count = 0
                    print(f"[INFO] {board} SCROLL STOP TRIGGER IS ZERO NOW")

                if after_pins_count == before_pins_count:
                    pins_trigger_count += 1
                    print(f"[INFO] {board} PINS COUNT STOP TRIGGER COUNT INCREASED")
                else :
                    pins_trigger_count = 0
                    print(f"[INFO] {board} PINS COUNT STOP TRIGGER IS ZERO NOW")
        except Exception as e:
            print(f"[ERROR] IN SCROLL AND SCRAPE {str(e)}")
            print("[INFO] WAITING FOR 5 MINUTES")
            time.sleep(5*60)
            self.__scroll_and_scrape(board)

    def __get_true_pins_count(self, board):
        true_pins_count = None
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT pin_count FROM stage1 WHERE board_url=?",(board,)).fetchone()
                conn.commit()
                true_pins_count = cursor[0]
        except Exception as e :
            print(f"[ERROR] cannot get true pins count!, because of {e}")
            time.sleep(1)
            return self.__get_true_pins_count(board)
        return true_pins_count


    def run(self):
        board_list = self.__get_board_urls()
        if not board_list:
            print("[ERROR] THIS SEARCH TERM HAS NO BOARDS IN DB")
            return 
        # Starting DB connections and driver.
        self.__start_connections()
        for board in board_list:
            self.board_url = f"https://www.pinterest.com{board}"
            print(f"[INFO] IN BOARD: {board}")
            # Check if board already scrapped
            if 0.95*self.__get_true_pins_count(board) <= self.__count_pins_in_board():
                print(f"[INFO] BOARD {board} ALREADY SCRAPPED")
                continue
            
            self.__scroll_and_scrape(board)
            print(f"[INFO] BOARD {board} ; SCRAPPED {len(self.all_links)} ; TARGET {self.__get_true_pins_count(board)}")

        self.driver.close()
            
        return 


