from genericpath import isdir
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
failed_download_links = []

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
        global failed_download_links
        FOLDER_PATH = os.path.join(PARENT_FOLDER_PATH , f"images-{next_dataset_index():04n}")
        os.makedirs(FOLDER_PATH, exist_ok=True) 
        
        all_urls = database.get_all_image_urls()
        print(f"\n\n\nall images len: {len(all_urls)}\n\n\n")
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
                    self.download(url,out_folder)
                    return
                try_again -= 1
                if try_again == 0:
                    if (url not in failed_download_links):
                        failed_download_links.append(url)



class pins:
    def __init__(self) -> None:
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
        global failed_download_links
        self.is_done = False
        try:
            page = requests.get(pin_url)
        except Exception as e:
            print(e)
            return
        soup = BeautifulSoup(page.content,'html.parser')
        #we look for the image tag to get the src url for download
        img_elements = soup.find_all('img')
        img_link = ""
        #img_elements len is zero image is not found
        if(len(img_elements) == 0):
            if (pin_url not in failed_download_links):
                failed_download_links.append(pin_url)
            return
        else:
            img_link = img_elements[0].get('src')
        database.push_image_url_into_database(img_link)


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
                print(f"FILE NAMES IN RAR SUBFOLDER {filenames}")
                for filename in filenames:
                    #create complete filepath of file in directory
                    filePath = os.path.join(folderName, filename)
                    # Add file to zip
                    zipObj.write(filePath, os.path.basename(filePath))

class chrome:
    def initDriver(IS_HEADLESS=False) -> webdriver:
        # options = chrome_options()
        # options.add_experimental_option(
        #     "excludeSwitches", ["enable-automation"])
        # options.add_experimental_option('useAutomationExtension', False)
        # options.add_argument("--disable-blink-features")
        # options.add_argument("--disable-blink-features=AutomationControlled")
        # options.headless = IS_HEADLESS
        # prefs = {
        #     "profile.managed_default_content_settings.images": 2
        # }
        # options.add_experimental_option("prefs", prefs)
        # return webdriver.Chrome(service=chrome_service(
        #     ChromeDriverManager().install()), options=options)
        options = uc.ChromeOptions()
        user_data_dir = f"{os.getcwd()}/chrome"
        #options.add_argument("--remote-debugging-port=9222")
        #options.add_argument('--headless')
        return uc.Chrome(options=options,user_data_dir=user_data_dir)
    def upload_to_mega(self):
        global RAR_PATH
        driver = self.initDriver()
        driver.get(MEGA_FOLDER_LINK)
        try:
            element = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.NAME, "dashboard"))
            )
        except:
            print('Waitting Error!!')
        time.sleep(5)
        number = 0
        r = rar()
        for file in r.get_rar_path():
            random_name = RAR_PATH + '/' + str(database.get_search_term()) + "_" +\
                str(number) + '.rar'
            number += 1

            os.rename(file, random_name)
            driver.find_element(By.ID, "fileselect3").send_keys(random_name)

        count = 0
        while(1):
            x = driver.find_elements(
                By.CLASS_NAME, 'transfer-task-row upload sprite-fm-mono icon-up progress')
            y = driver.find_elements(
                By.CLASS_NAME, 'transfer-task-row upload')

            if(len(x) == 0 and len(y) == 0):
                count += 1
            else:
                count = 0
            if(count == 5):
                break
            time.sleep(2)
        input("ENTER WHEN DONE")
        driver.quit()


class Stage4: 
    def __init__(self) -> None:
        pass
    
    def display_report(self):
        global failed_download_links
        total_images = database.get_total_images()
        print(f"\n\n\n Scraper found {total_images} images\n")
        print(f"out of {total_images} images {total_images-len(failed_download_links)} downloaded")

    def run(self, maximum_scrape_theads = 2) -> None:
        global failed_download_links
        print("started stage 4")
        database.delete_pin_is_downloading()
        database.delete_pin_is_downloading_imageurl()
        pin_urls = database.get_pin_url()

        if(pin_urls == None):
            print("Scraped all image link")

        else:
            pin = {}
            temp_pin_urls = {}
            for x in range(maximum_scrape_theads):
                pin[x] = pins()
                temp_pin_urls[x] = None
            count = 0
            threads = []
            while(len(pin_urls) != 0):
                for x in range(maximum_scrape_theads):
                    time.sleep(1)

                    if(temp_pin_urls[x] != None):
                        for i in temp_pin_urls[x]:
                            database.set_pin_is_downloaded(i)
                        temp_pin_urls[x] = None

                    temp = []
                    for c in range(200):
                        try:
                            temp.append(pin_urls.pop())
                            print(count)
                            count += 1
                        except:
                            break
                    # pin[x].set_is_done(False)
                    th = multiprocessing.Process(
                        target=pin[x].scrape_image_url, args=(temp, ))
                    th.start()
                    temp_pin_urls[x] = temp
                    threads.append(th)
                for a in threads:
                    a.join()

            for x in range(maximum_scrape_theads):
                time.sleep(1)

                if(temp_pin_urls[x] != None):
                    for i in temp_pin_urls[x]:
                        database.set_pin_is_downloaded(i)
                        
        print("Downloading images...")
        i = images()
        i.download_all_images()

        r = rar()
        r.add_to_rar_file()
        print("finished stage 4")
        print(f"Failed download links: {failed_download_links}")
        self.display_report()
        # c = chrome()
        # c.upload_to_mega()
 

if __name__ == '__main__':

    stage4 = Stage4()     
    stage4.run()
