from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from genericpath import isdir
import json
import threading
from lxml import html
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
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from lxml import etree
from progress.bar import ChargingBar
from mega import Mega
import getpass
from progress.spinner import LineSpinner 
import speedtest
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
similar_pin_urls = []
is_file_upload_done = False

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
    def get_total_images():
        cmd = "select total_images from report LIMIT 1;"
        try:
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.execute(cmd)
                conn.commit()
                for i in cursor:
                    return i[0]
        except Exception as e:
            print(e)
            time.sleep(1)
            return database.get_total_images()

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
                similar_pin_urls.append(image_url)
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
    
    @staticmethod
    def save_failed_pin_link(failed_pin_link):
            try:
                with sqlite3.connect(DATABASE_PATH) as conn:
                  conn.execute("INSERT INTO failed_pin_links VALUES (:pin_link, :error)",failed_pin_link)
            except Exception as e:
                print(e)
                time.sleep(1)
                database.save_failed_pin_link(failed_pin_link)      

    @staticmethod
    def get_failed_pin_links():
        cmd = "select pin_url,error from failed_pin_links"
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

class images:
    def download_images(self,download_urls,progress_bar,folder_path):
        self.progress_bar = progress_bar
        os.makedirs(folder_path, exist_ok=True) 
        for ur in download_urls:
            self.download(ur, folder_path)
            
            
    def download(self, url , out_folder):
        try_again = 2
        while(bool(try_again)):
            file_path = os.path.join(out_folder , url.replace(":", "_").replace("/", "_"))         
            if(os.path.exists(file_path)):
                print("exists: ", url)
                return
            try:
                r = requests.get(url, stream=True, timeout=60)
                if (r.status_code == 200):
                    with open(file_path, 'wb') as f:
                        r.raw.decode_content = True
                        shutil.copyfileobj(r.raw, f)
                        self.progress_bar.next()
                        database.set_image_downloaded(url)
                        break
            except Exception as e:
                time.sleep(1)
                if(str(e).find("conn") != -1):
                    self.download(url,out_folder)
                    return
                try_again -= 1
                if try_again == 0:
                    database.save_failed_pin_link(url)
                



class pins:
    def __init__(self) -> None:
        self.pin_urls = None
        self.progress_bar = None

    def scrape_image_url(self, urls,progress_bar):
        self.pin_urls = urls
        self.progress_bar = progress_bar
        for pin_url in self.pin_urls:
           self.sub_scrape_image(pin_url)
        

    def sub_scrape_image(self, pin_url):
        self.progress_bar.bar_prefix = pin_url
        retries = 1
        try:
            page = requests.get(pin_url)
        except Exception as e:
            database.save_failed_pin_link({"pin_link":pin_url,"error":str(e)})
            return
        soup = BeautifulSoup(page.content,'html.parser')
        #we look for the image tag to get the src url for download
        img_elements = soup.find_all('img')
        #img_elements len is zero image is not found
        if len(img_elements) != 0:
            img_link = img_elements[0].get('src')
            database.push_image_url_into_database(img_link)
            self.progress_bar.next()
        else:
            if retries != 0:
                self.sub_scrape_image(pin_url)
                retries -= 1
            else:
                database.save_failed_pin_link({"pin_link":pin_url,"error":"No Image found"})


class rar:
    def get_rar_path(self):
        global RAR_PATH
        l = []
        rar_files = listdir(RAR_PATH)
        for i in rar_files:
            if(i[-4:] == '.rar'):
                l.append(RAR_PATH+"\\"+i)
        return l

    def add_to_rar_file(self):

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
                #print(f"FILE NAMES IN RAR SUBFOLDER {filenames}")
                for filename in filenames:
                    #create complete filepath of file in directory
                    filePath = os.path.join(folderName, filename)
                    # Add file to zip
                    zipObj.write(filePath, os.path.basename(filePath))

class Mega_:
    def upload(self):
        try:
            mega_conf = json.load(open("mega_conf.json"))
            email = ""
            password = ""
            if mega_conf["email"] == "" or mega_conf["password"] == "":
                    email = input("Enter Mega Email: ")
                    password = getpass.getpass("Enter Mega Password: ")
            mega = Mega()
            user = mega.login(email, password)
            progress_indicator = threading.Thread(target = self.show_upload_progress)
            progress_indicator.start()
            user.upload(latest_file(RAR_PATH))
            is_file_upload_done = True
            progress_indicator.join()
            print("\nFile uploaded.")
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            return

    def show_upload_progress(self):
        progress = LineSpinner(message=f"Uploading {self.get_uploading_file_name()} to mega {self.get_uploading_file_size()} Mb: ")
        while(not is_file_upload_done):
            progress.next()

    def get_uploading_file_size(self):
        file_size_in_mb = os.path.getsize(latest_file(RAR_PATH))/1000000
        return "{:.2f}".format(file_size_in_mb)

    def get_uploading_file_name(self):
        return os.path.basename(latest_file(RAR_PATH))

class Stage3: 
    def __init__(self) -> None:
        pass
    
    def display_report(self):
        total_images = database.get_total_images()
        failed_links = database.get_failed_pin_links()
        print(f"Scraper found {total_images} images")
        print(f"{len(similar_pin_urls)} similar images found")
        print(f"Out of {total_images} images {total_images-len(failed_links)-len(similar_pin_urls)} downloaded")
        res = input("Display more info on errors? (y/n) ")
        if res == 'y':
            print("Similar Images Link: ")
            print(similar_pin_urls)
            if len(failed_links) == 0:
                print("No errors.")
            else:
                print("Failed links: ")
                print(failed_links)

    def run(self, maximum_scrape_theads = 4) -> None:
        global failed_download_links
        print("\nStarted stage 4")
        database.delete_pin_is_downloading()
        database.delete_pin_is_downloading_imageurl()
        pin_urls = database.get_pin_url()

        if(pin_urls == None):
            print("Scraped all image link")

        else:
            print(f"Scraping pins for image source link...")
            pin_progress_bar = ChargingBar("",max=len(pin_urls),suffix='%(percent)d%% - %(index)d/%(max)d')
            pin = pins()
            with ThreadPoolExecutor(max_workers=maximum_scrape_theads) as executor:
                for i in range(0,len(pin_urls),20):
                    executor.submit(pin.scrape_image_url,pin_urls[i:i+20],pin_progress_bar)
                        
        print("\n")
        folder_path = os.path.join(PARENT_FOLDER_PATH , f"images-{next_dataset_index():04n}")
        image = images()
        download_urls = database.get_all_image_urls()
        download_progress_bar = ChargingBar("Downloading images",max=len(download_urls),suffix='%(percent)d%% - %(index)d/%(max)d')
        with ThreadPoolExecutor(max_workers=maximum_scrape_theads) as executor:
                for i in range(0,len(download_urls),20):
                    executor.submit(image.download_images,download_urls[i:i+20],download_progress_bar,folder_path)
        

        r = rar()
        r.add_to_rar_file()

       
        print("Finished stage 4")
        self.display_report()
 

if __name__ == '__main__':
    stage3 = Stage3()
    stage3.run()
