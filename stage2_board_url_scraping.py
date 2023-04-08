import sqlite3
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
import time
import os 
from helper_functions import init_driver, create_database, save_html_page
import socket
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG)
DATABASE_PATH = "database.db"
SCROLL_IDLE_TIME = 3
TRIGGER_STOP = 7 # If your internet connection is slow, increase this number.
TIMEOUT = 10


class Stage2: 
    def __init__(self,search_term):
        self.search_term = search_term
        self.db_conn = None
        self.all_links = []
        self.scrapped_boards_count = 0 

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
            logging.info(f"[ERROR] cannot count pins in stage2 in board {self.board_url}!, because of {e}")
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
            logging.info(f"[ERROR] cannot check board existance , because of {e}")
            time.sleep(1)
            return self.__exist_in_db(pin_url)
        return exist

    def __push_to_database(self, pin_url):
        if self.__exist_in_db(pin_url):
            #logging.info(f"[INFO] {pin_url} with {self.board_url} exists.")
            return 
        else:
            #logging.info(f"[INFO] inserting {pin_url} with {self.board_url}.")
            self.__insert_to_database(pin_url)

    def __insert_to_database(self, pin_url):
        cmd = "insert into stage2(board_url, pin_url) values ('" + \
            str(self.board_url)+"','"+str(pin_url)+"')"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
                logging.info(f"[INFO] INSERTING {pin_url}")
        except Exception as e:
            if(str(e).lower().find("unique") != -1):
                pass
            elif(str(e).lower().find("database is locked") != -1):
                time.sleep(1)
                self.__push_to_database(pin_url)
            else:
                logging.info(str(e))


    def __get_board_urls(self):
        returns = []
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT board_url from stage1 WHERE search_term=?",(self.search_term,))
                conn.commit()
                returns.extend(url[0] for url in cursor)
        except Exception as e :
            logging.info(f"[ERROR] cannot get the boards urls!, because of {e}")
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
    
    def __load_page(self, url, max_attempts=5, timeout=20):
        """ moving to an exact web page and make sure it is loaded."""
        for attempt in range(1, max_attempts+1):
            logging.info(f"Attempt #{attempt} to load {url}")
            # Navigate to the URL
            self.driver.delete_all_cookies()
            self.driver.get(url)        
            self.driver.execute_script("document.body.style.zoom='50%'")

            # Wait for the page to load
            try:
                element_present = EC.presence_of_element_located((By.XPATH, "//div[@id='__PWS_ROOT__']"))
                WebDriverWait(self.driver, timeout).until(element_present)
                logging.info("Page loaded successfully")
                return True # page loaded successfully indicator.
            except TimeoutException:
                if attempt < max_attempts:
                    logging.info("Timed out waiting for page to load. Retrying...")
                else:
                    logging.info("Exceeded max attempts. Giving up.")

                    return False # page doesnot loaded successfully indicator.

    def __scrolling_boards(self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        for div_tag in soup.find_all("div"):
            try:
                if div_tag["data-test-id"] == "board-section":
                    return True
            except Exception as e:
                continue    
        return False


    def __scroll_and_scrape(self, board):
        try:
            logging.info(f"[INFO] STARTING SCROLLING AND SCRAPPING FOR BOARD: {board}")
            self.all_links = []

            page_loaded = self.__load_page(self.board_url)
            if not page_loaded:
                return 

            target_number_of_pins = self.__get_true_pins_count(board)
            scroll_trigger_count = 0
            pins_trigger_count = 0 
            while scroll_trigger_count <= TRIGGER_STOP and pins_trigger_count <= TRIGGER_STOP:

                before_pins_count = len(self.all_links)
                before_scroll_height = self.__get_scroll_height()

                logging.info(f"[INFO] {board} SCROLLING")
                self.__scroll_inner_height()
                time.sleep(SCROLL_IDLE_TIME)
                # Scrapping the pins.
                try:
                    self.__get_link_pin()
                except Exception as e:
                    if not self.__is_netwrok_available():
                        logging.info("[ERROR] NETWORK ERROR; PLEASE CHECK")
                        logging.info("[INFO] RESTARTING THE SCROLL AND SCRAPPING FUNCTION")
                        self.__scroll_and_scrape(board)                    

                    if self.__scrolling_boards():                        
                        pins_trigger_count = 0 
                        scroll_trigger_count = 0
                        logging.info("[INFO] SCROLLING BOARDS; NO PINS AVAILABLE TILL NOW")
                        continue
                    
                    logging.info(f"[ERROR] IN GETTING PINS; {e}")
                    save_html_page(self.board_url, f"{self.board_url}_stage2_error.html")
                    logging.info(f"[ERROR] CHECK HTML PAGE: {self.board_url}_stage2_error.html")
                    logging.info(f"[ERROR] IN BOARD {board} IN GETTING PINS")
                    logging.info(f"[INFO] ENDING {board}")
                    return

                logging.info(f"[INFO]{board} :: NUMBER OF PINS SCRAPPED: {len(self.all_links)} PINS OUT OF {target_number_of_pins}")
                logging.info(f"[INFO]{board} :: SCROLLING")           
                logging.info(f"[INFO] NUMBER OF BOARDS SCRAPPED {self.scrapped_boards_count}")
                self.__scroll_inner_height()
                time.sleep(SCROLL_IDLE_TIME)
                # Scrapping the pins.
                try:
                    self.__get_link_pin()
                except Exception as e:
                    if not self.__is_netwrok_available():
                        logging.info("[ERROR] NETWORK ERROR; PLEASE CHECK")
                        logging.info("[INFO] RESTARTING THE SCROLL AND SCRAPPING FUNCTION")
                        self.__scroll_and_scrape(board)                    

                    if self.__scrolling_boards():
                        pins_trigger_count = 0 
                        scroll_trigger_count = 0
                        logging.info("[INFO] SCROLLING BOARDS; NO PINS AVAILABLE TILL NOW")
                        logging.info(f"[INFO] BEFORE SCROLL HEIGHT: {before_scroll_height}")
                        continue
                    
                    logging.info(f"[ERROR] IN GETTING PINS; {e}")
                    save_html_page(self.board_url, f"{self.board_url}_stage2_error.html")
                    logging.info(f"[ERROR] CHECK HTML PAGE: {self.board_url}_stage2_error.html")
                    logging.info(f"[ERROR] IN BOARD {board} IN GETTING PINS")
                    logging.info(f"[INFO] ENDING {board}")
                    return

                logging.info(f"[INFO]{board} :: NUMBER OF PINS SCRAPPED: {len(self.all_links)} PINS OUT OF {target_number_of_pins}")                
                after_pins_count = len(self.all_links)

                logging.info(f"[INFO] {board} NUMBER OF PINS SCRAPPED: {after_pins_count} PINS OUT OF {target_number_of_pins}")            
                after_scroll_height = self.__get_scroll_height()

                # Check if the scroll height is the same
                if before_scroll_height == after_scroll_height:
                    scroll_trigger_count += 1
                    logging.info(f"[INFO] {board} SCROLL STOP TRIGGER COUNT INCREASED")
                else :
                    scroll_trigger_count = 0
                    logging.info(f"[INFO] {board} SCROLL STOP TRIGGER IS ZERO NOW")

                if after_pins_count == before_pins_count:
                    pins_trigger_count += 1
                    logging.info(f"[INFO] {board} PINS COUNT STOP TRIGGER COUNT INCREASED")
                else :
                    pins_trigger_count = 0
                    logging.info(f"[INFO] {board} PINS COUNT STOP TRIGGER IS ZERO NOW")
        except Exception as e:
            logging.info(f"[ERROR] IN SCROLL AND SCRAPE {str(e)}")
            logging.info("[INFO] WAITING FOR 5 MINUTES")
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
            logging.info(f"[ERROR] cannot get true pins count!, because of {e}")
            time.sleep(1)
            return self.__get_true_pins_count(board)
        return true_pins_count

    def run(self):
        board_list = self.__get_board_urls()
        if not board_list:
            logging.info("[ERROR] THIS SEARCH TERM HAS NO BOARDS IN DB")
            return 
        # Starting DB connections and driver.
        self.__start_connections()
        
        for board in board_list:
            self.board_url = f"https://www.pinterest.com{board}"
            logging.info(f"[INFO] IN BOARD: {board}")
            # Check if board already scrapped
            if 0.95*self.__get_true_pins_count(board) <= self.__count_pins_in_board():
                logging.info(f"[INFO] BOARD {board} ALREADY SCRAPPED")
                self.scrapped_boards_count += 1
                continue
            
            self.__scroll_and_scrape(board)
            logging.info(f"[INFO] BOARD {board} ; SCRAPPED {len(self.all_links)} ; TARGET {self.__get_true_pins_count(board)}")
            self.scrapped_boards_count += 1
        self.driver.close()
            
        return 


