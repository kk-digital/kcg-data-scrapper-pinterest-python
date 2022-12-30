from selenium.webdriver.common.by import By
import time 
from helper_functions import init_driver
from bs4 import BeautifulSoup
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# constants
SCROLL_IDLE_TIME = 0.5
POLL_TIME = 1
SCROLL_TIMEOUT = 10
TRIGGER_STOP = 5 # If your internet connection is slow, increase this number.

# Stage 1 class 
class Stage1:
    def __init__(self, driver):
        self.driver = driver
        self.all_data = []
        
    def __find_comma(self,str):
        """Find comma in the pins number to switch it in integer."""
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

    def __get_image_count(self,soup_a):
        """ getting the number of images in a certain board"""
        re = ''
        for i in soup_a.find_all("div"):
            try:
                if(i['style'] == '-webkit-line-clamp: 1;'):
                    re += i.text
            except Exception as e:
                pass
        re = re.replace('\n', '')
        if(re[self.__find_comma(re)+1:re.find("Pins")] == ''):
            return re
        return re[self.__find_comma(re)+1:re.find("Pins")]

    def __get_board_name(self,soup_a):
        for i in soup_a.find_all("div"):
            
            try:
                if(i['title'] != ''):
                    return i.text
            except Exception as e:
                continue


    def __get_boards(self, html):
        
        soup = BeautifulSoup(html, 'html.parser')

        div_list = soup.find_all("div")
        list_item_counter = 0 
        for div in div_list:
            try:
                if div["role"] == "listitem":
                    #print("list item found")
                    list_item_counter += 1 
            except:
                continue
        print(f"number of list items = {list_item_counter}")
        links_list = soup.find_all("a")

        for a in links_list:
            url = a["href"]
            image_count = self.__get_image_count(a)
            board_name =  self.__get_board_name(a)
            self.all_data.append({'url':url,'image_count':image_count,'board_name':board_name})
        
        print(f"scrapped links:  {len(links_list)}")
        return len(links_list)

    def scrape_boards_urls(self, height):
        tag_div = self.driver.find_element(By.XPATH , "//div[@class='gridCentered']")
        #tag_div = self.driver.find_element(By.TAG_NAME, "html")
        return self.__get_boards(tag_div.get_attribute('innerHTML'))


def get_scroll_height(driver):
    return driver.execute_script("return document.documentElement.scrollHeight")

def scroll_down(driver):
    scroll_height = get_scroll_height(driver) 
    driver.execute_script(f"window.scrollBy(0,{scroll_height});")
    #driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight-5) );")

    
def get_page_hash(driver):
    dom = driver.find_element(By.TAG_NAME,'html').get_attribute('innerHTML')
    return hash(dom.encode('utf-8'))


def main(search_term):
    driver = init_driver()
    driver.delete_all_cookies()
    driver.execute_script("document.body.style.zoom='50%'")
    driver.maximize_window()
    search_term = search_term.replace(" ", "%20")
    driver.get(f"https://www.pinterest.com/search/boards/?q={search_term}&rs=filter")
    driver.set_script_timeout(SCROLL_TIMEOUT)

    board_url_stage = Stage1(driver=driver)

    timeout_counter = 0
    trigger_counter = 0

    while True:
        page_hash = get_page_hash(driver)
        old_height = get_scroll_height(driver)

        try:
            scroll_down(driver=driver)
            time.sleep(SCROLL_IDLE_TIME)
            scroll_down(driver=driver)
            time.sleep(SCROLL_IDLE_TIME)
            
            time.sleep(POLL_TIME) # wait 0.5 seconds and check 

        except TimeoutError:
            print("[WARNING] TimeOut !")
            timeout_counter += 1
            continue

        new_height   = get_scroll_height(driver)
        page_hash_new = get_page_hash(driver)

        print(f"[INFO] OLD HEIGHT: {old_height} , NEW HEIGHT: {new_height}")

        if new_height == old_height:
            print("[INFO] SAME HEIGHT")
            trigger_counter += 1
        else:
            trigger_counter = 0  

        if page_hash_new == page_hash: 
            print("[WARNING] SAME HASH - SAME PAGE")
        else:
            print("[INFO] NEW PAGE LOADED")

        if trigger_counter == TRIGGER_STOP and page_hash_new == page_hash:
            print("[INFO] ALL PAGE LOADED")
            board_url_stage.scrape_boards_urls(new_height)
            break
        


if __name__ == "__main__":
    main(search_term="bears")