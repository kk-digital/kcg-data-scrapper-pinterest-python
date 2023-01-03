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



# Paths will be used in the script
out_folder = 'outputs'
os.makedirs(out_folder, exist_ok=True)
PARENT_FOLDER_PATH = os.path.join(out_folder, 'datasets')
os.makedirs(PARENT_FOLDER_PATH, exist_ok=True)
FOLDER_PATH = "" 
RAR_PATH = os.path.join(out_folder, 'dataset-zip-files')
os.makedirs(RAR_PATH, exist_ok=True)
maximum_scrape_theads = 2
maximum_download_theads = 40
DATABASE_PATH = 'database.db'
MEGA_FOLDER_LINK = ""


def next_dataset_index():
    """ Getting the index of the new dataset """
    try:
        indices = []
        for dataset in os.listdir(PARENT_FOLDER_PATH):
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
        
def latest_file(folder):
    list_of_files = glob.glob(f'{folder}/*') # * means all 
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

class database:
    @staticmethod
    def get_pins_for_board(board_url):
        returns= []
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT pin_url from stage2 WHERE board_url=?",(board_url,)).fetchall()
                conn.commit()
                for i in cursor:
                    returns.append(i[0])

        except Exception as e :
            print(f"[ERROR] cannot count pins in stage2 in board {board_url}!, because of {e}")
            time.sleep(1)
            return database.count_pins_in_board(board_url)
        return returns


    @staticmethod
    def get_board_urls(search_term):
        """returns a list of boards urls from stage1 table using the search term """
        returns = []
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT board_url from stage1 WHERE search_term=?",(search_term,))
                conn.commit()
                for i in cursor:
                    returns.append(i[0])

        except Exception as e :
            print(f"[ERROR] cannot get the boards urls!, because of {e}")
            time.sleep(1)
            return database.get_board_urls(search_term)
        return returns

    @staticmethod
    def get_search_term():
        cmd = "select search_term from stage1 LIMIT 1;"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute(cmd)
                conn.commit()
                for i in cursor:
                    return i[0]
        except:
            time.sleep(1)
            return database.get_search_term()

    @staticmethod
    def push_image_url_into_database(image_url):
        cmd = "insert into image_url(url) values ('"+image_url+"')"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            if(str(e).lower().find("unique") != -1):
                pass
            elif(str(e).lower().find("database is locked") != -1):
                time.sleep(1)
                database.push_image_url_into_database(image_url)
            else:
                print(str(e))

    @staticmethod
    def set_pin_is_downloaded(pin_url):
        cmd = "update stage2 set downloaded = 2 where pin_url = '"+pin_url+"'"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            print(str(e))
            if(str(e).find('database disk image is malformed') != -1):
                input()
            time.sleep(1)
            database.set_pin_is_downloaded(pin_url)

    @staticmethod
    def delete_pin_is_downloading():
        """ Making all the pins (not downloaded) """

        cmd = "update stage2 set downloaded = 0 where downloaded != 0"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            print(str(e))
            time.sleep(1)
            database.delete_pin_is_downloading()

    @staticmethod
    def delete_pin_is_downloading_imageurl():
        """ Making all the pins (not downloaded) """

        cmd = "update image_url set downloaded = 0 where downloaded != 0"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            print(str(e))
            time.sleep(1)
            database.delete_pin_is_downloading()


    @staticmethod
    def set_url_downloading(url):
        try:
            cmd2 = "update stage2 set downloaded = 1 where pin_url = '"+url+"'"
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd2)
                conn.commit()
        except Exception as e:
            print(str(e))
            time.sleep(1)
            database.set_url_downloading(url)

    @staticmethod
    def get_pin_url():
        """ Getting all the pin urls for images which will be downloaded (download state = 0) """

        cmd_get_url_list = "select pin_url from stage2 where downloaded = 0;"
        # cmd_get_url_list = "select pin_url from stage2 where downloaded = 0 LIMIT 1000;"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute(cmd_get_url_list)
                conn.commit()
                pin_temp = []
                for i in cursor:
                    for x in i:
                        pin_temp.append(x)
                if(len(pin_temp) == 0):
                    return None
                return pin_temp
        except Exception as e:
            print(str(e))
            time.sleep(1)
            return database.get_pin_url()

    @staticmethod
    def get_all_image_urls():
        cmd = "select url from image_url where downloaded = 0"
        returns = []
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute(cmd)
                conn.commit()
                for i in cursor:
                    returns.append(i[0])
                return returns
        except Exception as e:
            print(str(e))
            time.sleep(1)
            database.get_all_image_urls()

    @staticmethod
    def set_image_downloaded(ur):
        cmd = "update image_url set downloaded = 1 where url = '"+ur+"';"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute(cmd)
                conn.commit()
        except Exception as e:
            print(str(e))
            time.sleep(1)
            database.set_image_downloaded(ur)


class images:
    def download_all_images(self):
        
        FOLDER_PATH = os.path.join(PARENT_FOLDER_PATH , f"images-{next_dataset_index():04n}")
        os.makedirs(FOLDER_PATH, exist_ok=True) 
        
        all_urls = database.get_all_image_urls()
        threads = []
        count = 0
        for ur in all_urls:
            count += 1
            th = multiprocessing.Process(
                target=self.download, args=(ur, FOLDER_PATH))
            th.start()
            threads.append(th)
            database.set_image_downloaded(ur)
            if(count % maximum_download_theads == 0):
                for i in threads:
                    i.join()
                threads = []

    def download(self, url , out_folder):
        try_again = 2

        while(bool(try_again)):
            file_path = os.path.join(out_folder , url.replace(":", "_").replace("/", "_"))
            print(f"[INFO] downloading :{file_path} ")
            
            if(os.path.exists(file_path)):
                print("exists: ", url)
                return
            try:
                r = requests.get(url, stream=True, timeout=60)
                if (r.status_code == 200):
                    with open(file_path, 'wb') as f:
                        r.raw.decode_content = True
                        shutil.copyfileobj(r.raw, f)
                break
            except Exception as e:
                print(str(e))
                time.sleep(1)
                if(str(e).find("conn") != -1):
                    self.download(url, out_folder)
                    return
                try_again -= 1


class pins:
    def __init__(self):
        self.pin_urls = None
        self.is_done = True

    def scrape_image_url(self, urls):
        self.pin_urls = urls
        self.is_done = False
        threads = []
        for pin_url in self.pin_urls:
            th = multiprocessing.Process(
                target=self.sub_scrape_image, args=(pin_url, ))
            th.start()
            threads.append(th)
        for t in threads:
            t.join()
        self.is_done = True

    def sub_scrape_image(self, pin_url):
        self.is_done = False
        try:
            page = requests.get(pin_url).text
            soup = BeautifulSoup(page, 'html.parser')
            image_url = soup.find("img")["src"]
            database.push_image_url_into_database(image_url)

        except Exception as e:
            print(str(e))
            if(str(e).find("conn") != -1):
                self.sub_scrape_image(pin_url)
                print(f"[WARNING] {pin_url} failed; error: {e}")
                return
            print(f"[WARNING] {pin_url} failed; error: {e}")

        return
        
class rar:
    def get_rar_path(self):
        global RAR_PATH
        l = []
        rar_files = listdir(RAR_PATH)
        for i in rar_files:
            if(i[-4:] == '.rar'):
                l.append(RAR_PATH+"\\"+i)
        return l

    def add_to_zip_file(self):

        os.makedirs(RAR_PATH, exist_ok=True)
        FOLDER_PATH = latest_file(PARENT_FOLDER_PATH)
        
        print(f"[INFO] FOLDER WILL BE ZIPPED : {FOLDER_PATH}")
        
        files = [f for f in listdir(FOLDER_PATH) if isfile(join(FOLDER_PATH, f))]
        
        print(f"[INFO] {len(files)} will be zipped ")
        
        self.create_rar_file(FOLDER_PATH, os.path.basename(FOLDER_PATH))

    def copy_to(self, from_file, to_folder):
        try: 
            shutil.copy2(from_file, to_folder)
        except Exception: 
            pass 

    def create_rar_file(self, sub_folder: str , zip_file_name: str) -> None:
        '''method that writes all files inside the `sub_folder` into a zip file in the working directory with the name of `zip_file_name` 
        :param sub_folder: The directory to get the it's files paths
        :type sub_folder: str
        :param zip_file_name: If it's set to True the function will return paths of all files in the given directory 
                and all its subdirectories
        :type zip_file_name: str
        :returns: 
        :rtype: None 
        '''
        #make sure the last zip file name has the correct extension 
        _ , file_extension = os.path.splitext(zip_file_name)
        if file_extension != '.zip': 
            zip_file_name += '.zip'
            
        # create a ZipFile object
        with ZipFile(os.path.join(RAR_PATH,zip_file_name), 'w') as zipObj:
            # Iterate over all the files in directory
            for folderName, subfolders, filenames in os.walk(sub_folder):
                print(f"FILE NAMES IN RAR SUBFOLDER {filenames}")
                for filename in filenames:
                    #create complete filepath of file in directory
                    filePath = os.path.join(folderName, filename)
                    # Add file to zip
                    zipObj.write(filePath, os.path.basename(filePath))


class Stage3: 
    def __init__(self, search_term,max_workers=10):
        self.max_workers = max_workers
        self.search_term = search_term 
        self.board_pins_dict = {}
        self.scraped_pin_url = {}
        self.parent_directory  = os.path.join('outputs','datasets')
        self.zip_output_folder = os.path.join('outputs','dataset-zip-files') 
        folder_index = self.__next_dataset_index()
        self.folder_name = f"images-{folder_index:04n}"
        self.output_folder = os.path.join(self.parent_directory,self.folder_name)
        self.__create_folders()

    def __create_folders(self):
        os.makedirs(self.parent_directory, exist_ok=True)               
        os.makedirs(self.zip_output_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)

    
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
            os.makedirs(os.path.join(self.output_folder,board_name),exist_ok=True)               
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for pin in self.board_pins_dict[board]:
                    image_url = self.__get_image_url_from_pin(pin)
                    if image_url is None:
                        continue
                    a_result = executor.submit(self.__download_image, board_name,image_url)
                    futures.append(a_result)
        
                for future in concurrent.futures.as_completed(futures):
                    future.result()

    def __download_image(self,board_name,image_url):
        file_path = os.path.join(self.output_folder,board_name,image_url.replace(":", "_").replace("/", "_"))
        if(os.path.exists(file_path)):
            print(f"[INFO] {image_url} ALREADY EXISTS")
            return

        print(f"[INFO] DOWNLOADING: {image_url} ")
        no_of_tries = 0 
        try:
            r = requests.get(image_url, stream=True, timeout=60)
            if (r.status_code == 200):
                with open(file_path, 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)
        except Exception as e:
            time.sleep(1)
            if no_of_tries == 0:
                print(f"[WARNING] {image_url} NOT DOWNLOADED -- TRYING AGAIN, {e}")
                self.__download_image(image_url)
                no_of_tries += 1 
            else:
                print(f"[ERROR] {image_url} WAS NOT DOWNLOADED, {e}")

    def __get_image_url_from_pin(self, pin_url):
        no_of_tries = 0 
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
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

    def __update_pin(self,pin_url,image_url):
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute("UPDATE image_url SET img_url=? WHERE pin_url=?",(image_url,pin_url))
                conn.commit()
                print(f"[INFO] IMAGE URL UPDATED SUCCESSFULLY FOR PIN {pin_url}")
        except Exception as e:
            if "database is locked" in str(e).lower() :
                time.sleep(1)
                self.__update_pin(pin_url,image_url)
            else:
                print(f"[WARNING] {pin_url} CANNOT BE UPDATED TO DB.")

    def __insert_into_db(self, pin_url, image_url):
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                conn.execute("INSERT INTO image_url (pin_url, img_url) values (?,?)",(pin_url,image_url))
                conn.commit()
                print(f"[INFO] IMAGE URL INSERTED SUCCESSFULLY FOR PIN {pin_url}")
        except Exception as e:
            if "database is locked" in str(e).lower() :
                time.sleep(1)
                self.__insert_into_db(pin_url,image_url)
            else:
                print(f"[WARNING] {pin_url} CANNOT BE INSERTED TO DB.")
 
    def __check_existance(self, pin_url, image_url):
        exist = 0
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
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
                print(f"[WARNING] UPDATING {pin_url} IN DB")
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
            if(str(e).find("conn") != -1):
                self.__scrape_image_url(pin_url)
            if no_of_tries == 0:
                self.__scrape_image_url(pin_url)
                no_of_tries += 1
            print(f"[WARNING] {pin_url} FAILED, {e}")
            return None , None 

    def __count_pins_in_board(self,board_url):
        pins_count = None
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
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
            board_name = board.split('/')[-2]
            board_image_list = os.listdir(os.path.join(self.output_folder, board_name))
            board_url = f"https://www.pinterest.com{board}"
            scrapped_pins_count = self.__count_pins_in_board(board_url)
            print(f"[INFO]  {len(board_image_list)} OUT OF {scrapped_pins_count} IMAGES DOWNLOADED FOR BOARD {board_name}")
        print("-"*100)

    def __count_unique_image_urls(self):
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute("SELECT COUNT(DISTINCT img_url) as unique_count FROM image_url").fetchone()
                conn.commit()
                return cursor[0]

        except Exception as e :
            print(f"[ERROR] CANNOT COUNT UNIQUE IMAGE URLS")
            time.sleep(1)
            return self.__count_unique_image_urls()
        return exist

    def __create_zip_file(self):
        shutil.make_archive(os.path.join(self.zip_output_folder,self.folder_name), 'zip', self.output_folder)

    def run(self):
        # list of all boards urls        
        board_urls = database.get_board_urls(search_term=self.search_term)
        for board in board_urls:
            # get all the pins of a board url from stage2 table 
            self.board_pins_dict[board] = database.get_pins_for_board(f"https://www.pinterest.com{board}")
            # looping on every pin url
            # for pin in self.board_pins_dict[board]:
            #     print(f"[INFO] FINDING IMAGE URL FOR PIN: {pin}")
            #     self.__scrape_image_url(pin)

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for pin in self.board_pins_dict[board]:
                    print(f"[INFO] FINDING IMAGE URL FOR PIN: {pin}")
                    a_result = executor.submit(self.__scrape_image_url, pin)
                    futures.append(a_result)
        
                for future in concurrent.futures.as_completed(futures):
                    pin_url , image_url = future.result()
                    if pin_url is not None:
                        self.scraped_pin_url[pin_url] = image_url

        print("[INFO] INSERTING TO DB") 
        self.__push_pin_image_url_to_database()

        print("[INFO] STARTING DOWNLOADING ...")
        self.__download_all_images()
        print("[INFO] DOWNLOAD ENDED")

        print("[INFO] REPORTING")
        self.__report()

        print("[INFO] CREATING ZIP FILE")
        self.__create_zip_file()


if __name__ == '__main__':

    stage4 = Stage3("kcg-characters")     
    stage4.run()
