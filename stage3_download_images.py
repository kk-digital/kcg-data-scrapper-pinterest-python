from genericpath import isdir
import re
import sqlite3
import subprocess
import requests
import time
import multiprocessing
import os
import shutil
from os import listdir
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as chrome_service
from selenium.webdriver.chrome.options import Options as chrome_options
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from os.path import isfile, join
from zipfile import ZipFile
import glob
from bs4 import BeautifulSoup
import concurrent.futures

DATABASE_PATH = 'database.db'

class Stage3: 
    def __init__(self, search_term,max_workers=10):
        self.max_workers = max_workers
        self.search_term = search_term 
        self.board_pins_dict = {}
        self.report_dict = {} # a dictionary which each board has it's stats in dict.
        self.scraped_pin_url = {}
        self.parent_directory  = os.path.join('outputs','datasets')
        self.zip_output_folder = os.path.join('outputs','dataset-zip-files')
        os.makedirs(self.parent_directory, exist_ok=True)               
        os.makedirs(self.zip_output_folder, exist_ok=True)
        folder_index = self.__next_dataset_index()
        self.folder_name = f"images-{folder_index:04n}"
        self.output_folder = os.path.join(self.parent_directory,self.folder_name)
        os.makedirs(self.output_folder, exist_ok=True)
        self.unique_files = {} # 'board':[List of unique files]
        self.db_conn  = None
        self.db_conn  = self.__initiate_db_conn() 


    def __initiate_db_conn(self):
        """ getting the connection object to the DB""" 
        if not os.path.exists(DATABASE_PATH):
            print(f"[ERROR] no db")       
            return SyntaxError     

        return sqlite3.connect(DATABASE_PATH)

    def __next_dataset_index(self):
        """ Getting the index of the new dataset """
        try:
            indices = []
            for dataset in os.listdir(self.parent_directory):
                try :
                    indices.append(int(dataset.split("-")[1]))
                    continue
                except Exception as e:
                    print(f"[WARINING] {dataset} has a problem")            
                    try:
                        indices.append(max(indices)+1)
                    except ValueError: # indices list is empty 
                        indices.append(1)
            return max(indices) + 1
        except ValueError:
            return 1    
        
    def __download_all_images(self):
        for board in self.board_pins_dict:
            board_name = board.split('/')[-2]
            print(f"[INFO] DOWNLOADING BOARD {board_name}")
            board_folder = "_".join([elem for elem in board.split("/") if elem != ''])
            os.makedirs(os.path.join(self.output_folder,board_folder),exist_ok=True)               
            self.unique_files[board_folder] = [] 

            # with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            #     futures = []
            #     for pin in self.board_pins_dict[board]:
                    
            #         try:
            #             image_url = self.scraped_pin_url[pin]
            #         except KeyError:
            #             #print(f"[WARNING] PIN {pin} WAS MISSED")
            #             continue

            #         if image_url is None:
            #             continue
            #         a_result = executor.submit(self.__download_image, board_folder,image_url)
            #         futures.append(a_result)
        
            #     for future in concurrent.futures.as_completed(futures):
            #         success_url = future.result()
            #         if success_url is not None:
            #             self.report_dict[board]['download_success_images'] += 1
    
            for pin in self.board_pins_dict[board]:
                
                try:
                    image_url = self.scraped_pin_url[pin]
                except KeyError:
                    #print(f"[WARNING] PIN {pin} WAS MISSED")
                    continue

                if image_url is None:
                    continue
    
                success_url = self.__download_image(board_folder,image_url)
                if success_url is not None:
                    self.report_dict[board]['download_success_images'] += 1
                
                        
    def __download_image(self,board_folder,image_url):
        file_path = os.path.join(self.output_folder,board_folder,image_url.replace(":", "_").replace("/", "_"))
        
        # check if the file exists in the folder or not 
        if file_path not in self.unique_files[board_folder]:
            self.unique_files[board_folder].append(file_path)
        else:
            return

        #print(f"[INFO] DOWNLOADING: {image_url} ")
        no_of_tries = 0 
        try:
            r = requests.get(image_url, stream=True, timeout=60)
            if (r.status_code == 200):
                with open(file_path, 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)
                f.close()

            return image_url
        except Exception as e:
            time.sleep(1)
            if no_of_tries == 0:
                print(f"[WARNING] {image_url} NOT DOWNLOADED -- TRYING AGAIN, {e}")
                self.__download_image(board_folder,image_url)
                no_of_tries += 1 
            else:
                print(f"[ERROR] {image_url} WAS NOT DOWNLOADED, {e}")
        
        return None

    def __get_image_url_from_pin(self, pin_url):
        no_of_tries = 0 
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT img_url FROM image_url WHERE pin_url=? ",(pin_url,)).fetchone()
                conn.commit()
                return cursor[0]

        except Exception as e:
            if no_of_tries == 0 :
                print(f"[WARNING] TRYING AGAIN -- CANNOT GET IMAGE URL FOR {pin_url}, {e}")
                time.sleep(1)
                return self.__get_image_url_from_pin(pin_url)
            else:
                print(f"[ERROR] CANNOT GET IMAGE URL FOR {pin_url}, {e}")

    def __get_pins_for_board(self,board_url):
        returns= []
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT pin_url from stage2 WHERE board_url=?",(board_url,)).fetchall()
                conn.commit()
                for i in cursor:
                    returns.append(i[0])

        except Exception as e :
            print(f"[ERROR] CANNOT GET PINS FOR BOARD: {board_url}!,{e}")
            time.sleep(1)
            return self.__get_pins_for_board(board_url)
        return returns

    def __get_true_pins_count(self,board):
        true_pins_count = None
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT pin_count FROM stage1 WHERE board_url=?",(board,)).fetchone()
                conn.commit()
                true_pins_count = cursor[0]

        except Exception as e :
            print(f"[ERROR] cannot get true pins count!, because of {e}")
            time.sleep(1)
            return self.__get_true_pins_count(board)
        return true_pins_count

    def __get_board_urls(self):
        """returns a list of boards urls from stage1 table using the search term """
        returns = []
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT board_url from stage1 WHERE search_term=?",(self.search_term,))
                conn.commit()
                for i in cursor:
                    returns.append(i[0])

        except Exception as e :
            print(f"[ERROR] CANNOT GET THE BOARDS URLS!, {e}")
            time.sleep(1)
            return self.__get_board_urls()
        return returns

    def __update_pin(self,pin_url,image_url):
        try:
            with self.db_conn as conn:
                conn.execute("UPDATE image_url SET img_url=? WHERE pin_url=?",(image_url,pin_url))
                conn.commit()
                #print(f"[INFO] IMAGE URL UPDATED SUCCESSFULLY FOR PIN {pin_url}")
        except Exception as e:
            if "database is locked" in str(e).lower() :
                time.sleep(1)
                self.__update_pin(pin_url,image_url)
            else:
                print(f"[WARNING] {pin_url} CANNOT BE UPDATED TO DB.")

    def __insert_into_db(self, pin_url, image_url):
        try:
            with self.db_conn as conn:
                conn.execute("INSERT INTO image_url (pin_url, img_url) values (?,?)",(pin_url,image_url))
                conn.commit()
                #print(f"[INFO] IMAGE URL INSERTED SUCCESSFULLY FOR PIN {pin_url}")
        except Exception as e:
            if "database is locked" in str(e).lower() :
                time.sleep(1)
                self.__insert_into_db(pin_url,image_url)
            else:
                print(f"[WARNING] {pin_url} CANNOT BE INSERTED TO DB.")
 
    def __check_existance(self, pin_url, image_url):
        exist = 0
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT count(*) FROM image_url WHERE pin_url=? AND img_url=?",(pin_url,image_url)).fetchone()
                conn.commit()
                exist = cursor[0]
        except Exception as e :
            print(f"[ERROR] CANNOT CHECK EXISTANCE OF {pin_url}::{image_url},{e}")
            time.sleep(1)
            return self.__check_existance(pin_url)
        return exist

    def __push_pin_image_url_to_database(self):
        for pin_url in self.scraped_pin_url:
            image_url = self.scraped_pin_url[pin_url]
            if self.__check_existance(pin_url, image_url):
                #print(f"[WARNING] UPDATING {pin_url} IN DB")
                self.__update_pin(pin_url,image_url)
            else:
                self.__insert_into_db(pin_url, image_url)    
    
    def __scrape_image_url(self, pin_url):
        """ getting the image url given pin url """
        no_of_tries = 0 # may be fail in first time for connection errors
        try:
            page = requests.get(pin_url).text
            soup = BeautifulSoup(page, 'html.parser')
            image_url = soup.find("img")["src"]
            return pin_url, image_url
            #self.__push_pin_image_url_to_database(pin_url,image_url)

        except Exception as e:
            if"conn" in str(e) :
                self.__scrape_image_url(pin_url)
            if no_of_tries == 0:
                time.sleep(2)
                no_of_tries += 1
                self.__scrape_image_url(pin_url)
            print(f"[WARNING] {pin_url} FAILED, {e}")
            return None , None 

    def __count_pins_in_board(self,board_url):
        pins_count = None
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT count(*) as pins_count from stage2 WHERE board_url=?",(board_url,)).fetchall()
                conn.commit()
                pins_count = cursor[0][0]

        except Exception as e :
            print(f"[ERROR] cannot count pins in stage2 in board {board_url}!, because of {e}")
            time.sleep(1)
            return self.__count_pins_in_board(board_url)
        return pins_count

    def __report(self):
        print("-"*100)
        for board in self.board_pins_dict:
            #board_url = f"https://www.pinterest.com{board}"
            board_folder = "_".join([elem for elem in board.split("/") if elem != ''])
            print(f"[INFO] BOARD: {board}")
            print(f"-------> TARGET: {self.__get_true_pins_count(board)}")
            print(f"-------> SCRAPPED: {self.report_dict[board]['scrapped_pin_count']}")
            print(f"-------> UNIQUE: {len(self.unique_files[board_folder])}")
            print(f"-------> DOWNLOADED: {self.report_dict[board]['download_success_images']}")
            print("-"*50)
        print("-"*100)

    def __count_unique_image_urls(self):
        try:
            with self.db_conn as conn:
                cursor = conn.execute("SELECT COUNT(DISTINCT img_url) as unique_count FROM image_url").fetchone()
                conn.commit()
                return cursor[0]
        except Exception as e :
            print(f"[ERROR] CANNOT COUNT UNIQUE IMAGE URLS")
            time.sleep(1)
            return self.__count_unique_image_urls()

    def __create_zip_file(self):
        shutil.make_archive(os.path.join(self.zip_output_folder,self.folder_name), 'zip', self.output_folder)
        print(f"[INFO] ZIP FILE CREATED SUCCESSFULLY, CHECK {self.folder_name}.zip")

    def run(self):
        # list of all boards urls        
        board_urls = self.__get_board_urls()
        for board in board_urls:
            # get all the pins of a board url from stage2 table 
            self.board_pins_dict[board] = self.__get_pins_for_board(f"https://www.pinterest.com{board}")
            self.report_dict[board] = {'scrapped_pin_count':0, 'download_success_images':0}
            print(f"[INFO] FINDING IMAGE URLS FOR PIN OF BOARD: {board}")
            # with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            #     futures = []
            #     for pin in self.board_pins_dict[board]:
            #         #print(f"[INFO] FINDING IMAGE URL FOR PIN: {pin}")
            #         a_result = executor.submit(self.__scrape_image_url, pin)
            #         futures.append(a_result)
        
            #     for future in concurrent.futures.as_completed(futures):
            #         pin_url , image_url = future.result()
                    
            #         if pin_url is not None:
            #             self.scraped_pin_url[pin_url] = image_url
            #             self.report_dict[board]['scrapped_pin_count'] += 1 
            for pin in self.board_pins_dict[board]:
                #print(f"[INFO] FINDING IMAGE URL FOR PIN: {pin}")
                pin_url , image_url = self.__scrape_image_url(pin)
                
                if pin_url is not None:
                    self.scraped_pin_url[pin_url] = image_url
                    self.report_dict[board]['scrapped_pin_count'] += 1 
                        
                    
        # print("[INFO] INSERTING TO DB") 
        # self.__push_pin_image_url_to_database()

        print("[INFO] STARTING DOWNLOADING ...")
        self.__download_all_images()
        print("[INFO] DOWNLOAD ENDED")

        print("[INFO] REPORTING")
        self.__report()

        print("[INFO] CREATING ZIP FILE")
        self.__create_zip_file()

# if __name__ == '__main__':

#     stage4 = Stage3("kcg-characters")     
#     stage4.run()
