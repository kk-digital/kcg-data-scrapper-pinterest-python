from selenium.webdriver.common.by import By
import time 
from helper_functions import init_driver
from bs4 import BeautifulSoup
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# global 
urls = []
all_data = []
        
# constants
SCROLL_IDLE_TIME = 3
POLL_TIME = 1
SCROLL_TIMEOUT = 10
TRIGGER_STOP = 3 # If your internet connection is slow, increase this number.
RESIZE_WAIT_TIME = 10
TIMEOUT = 10


def find_comma(str):
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

def get_image_count(soup_a):
    """ getting the number of images in a certain board"""
    re = ''
    for i in soup_a.find_all("div"):
        try:
            if(i['style'] == '-webkit-line-clamp: 1;'):
                re += i.text
        except Exception as e:
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
        except Exception as e:
            continue

def get_boards(html):
    
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
    out_urls = []
    links_list = soup.find_all("a")
    for a in links_list:
        url = a["href"]
        if url in out_urls:
            continue        
        image_count = get_image_count(a)
        board_name =  get_board_name(a)
        all_data.append({'url':url,'image_count':image_count,'board_name':board_name})
        out_urls.append(url)

    for data in all_data:
        print(data)

    print(f"scrapped links:  {len(links_list)}")
    print(f"number of list items = {list_item_counter}")
    return len(links_list)

def scrape_boards_urls(driver):
    tag_div = driver.find_element(By.XPATH , "//div[@class='gridCentered']")
    return get_boards(tag_div.get_attribute('innerHTML'))


def get_scroll_height(driver):
    return driver.execute_script("return document.documentElement.scrollHeight")

def get_page_current_width_height(driver):
    window_size = driver.get_window_size()
    return window_size['width'],window_size['height']

def get_page_inner_height(driver):
    return driver.execute_script("return window.innerHeight")

def scroll_down(driver):
    scroll_height = get_scroll_height(driver) 
    driver.execute_script(f"window.scrollTo(0, {scroll_height});")
    #driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight-5) );")



def get_page_hash(driver):
    dom = driver.find_element(By.TAG_NAME,'html').get_attribute('innerHTML')
    return hash(dom.encode('utf-8'))


def main(search_term):
    driver = init_driver()
    driver.delete_all_cookies()
    driver.maximize_window()
    search_term = search_term.replace(" ", "%20")
    driver.get(f"https://www.pinterest.com/search/boards/?q={search_term}&rs=filter")

    
    trigger_counter = 0
    while True:
        page_hash  = get_page_hash(driver)

        old_scroll_height = get_scroll_height(driver)
        scroll_down(driver)
        time.sleep(SCROLL_IDLE_TIME)
        scroll_down(driver=driver)
        time.sleep(SCROLL_IDLE_TIME)
        new_scroll_height = get_scroll_height(driver)

        print(f"[INFO] SCROLL  HEIGHT: {new_scroll_height}")

        # scroll timeout loop
        start_time = time.time()
        while True:
            time.sleep(POLL_TIME) # wait 0.5 seconds and check 

            if time.time() - start_time > TIMEOUT:
                print(f"[WARNING] NO CHANGE IN PAGE IN {TIMEOUT} SECONDS")
                break

            if old_scroll_height == new_scroll_height:
                print("[WARNING] SCROLL HEIGHT IS THE SAME")
                break
            
            if page_hash != get_page_hash(driver):
                print("[INFO] PAGE CHANGED")
                break


        if old_scroll_height == new_scroll_height and page_hash == get_page_hash(driver):
            trigger_counter += 1

        if trigger_counter == TRIGGER_STOP:
            width, _ = get_page_current_width_height(driver)
            height   = driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight );")
            driver.set_window_size(width=width, height=height) ## works also 
            time.sleep(RESIZE_WAIT_TIME)
            print(f"[INFO] ALL HEIGHT {height}")
            print("[INFO] ALL PAGE LOADED")
            scrape_boards_urls(driver)
            driver.close()
            break
        

if __name__ == "__main__":
    main(search_term="bears")