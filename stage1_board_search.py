import os
import sqlite3
from consts import DATABASE_PATH
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from helper_functions import init_driver , create_database, page_has_loaded


class Stage1:
    """ Stage1: getting all boards urls for a given search term"""

    def __init__(self, search_term):
        self.search_term = search_term
        self.all_data = {}
        self.db_conn  = self.__initiate_db_conn() 
        self.driver = init_driver()


    def __initiate_db_conn(self):
        """ getting the connection object to the DB""" 
        if not os.path.exists(DATABASE_PATH):
            create_database()

        return sqlite3.connect(DATABASE_PATH)
        

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
        links_list = soup.find_all("a")

        for a in links_list:
            url = a["href"]
            print(url)
            image_count = self.__get_image_count(a)
            board_name =  self.__get_board_name(a)
            self.all_data[url] = [self.search_term, image_count, board_name]
        
        return len(links_list)
    
    def __scrape_boards_urls(self):
        self.driver.maximize_window() # For maximizing window
        self.driver.implicitly_wait(20) # gives an implicit wait for 20 seconds
        tag_div = self.driver.find_element(By.XPATH , "//div[@role='list']")
        return self.__get_boards(tag_div.get_attribute('innerHTML'))
    
    def __scroll_and_scrape(self):        
        # make driver settings ready for scrapping.
        self.driver.delete_all_cookies()
        self.driver.get("https://www.pinterest.com/search/boards/?q="+self.search_term.replace(" ", "%20")+"&rs=filter")
        self.driver.execute_script("document.body.style.zoom='50%'")
        self.driver.maximize_window()

        # scrape boards url
        url_count = self.__scrape_boards_urls()
        # scroll 
        self.driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight-5) );")
        page_has_loaded(driver=self.driver)
        
        
        same_url_count = 0
        while same_url_count < 5 :
            new_url_count = self.__scrape_boards_urls()
            
            if new_url_count == url_count:
                same_url_count += 1

            url_count = new_url_count
            self.driver.execute_script("window.scrollBy(0, Math.abs(window.innerHeight-5) );")
            page_has_loaded(driver=self.driver)

    def __insert_data_into_database(self, board_url, images_count):
        try:
            self.db_conn.execute("""INSERT INTO stage1(search_term, board_url, pin_count) VALUES (?,?,?)""",
                            (self.search_term.replace("'", "''"), board_url, images_count),)
            self.db_conn.commit()
        
        except sqlite3.IntegrityError:
            print(f"[INFO] updating board : {board_url} in DB")
            self.db_conn.execute("UPDATE stage1 SET search_term = ?, pin_count = ? WHERE board_url = ?;",(self.search_term.replace("'", "''"), images_count, board_url))
            self.db_conn.commit()
        
        except Exception as e:
            print(f"[INFO] ERROR {e} in board {board_url}")
    
    def __exit_stage(self):
        self.db_conn.close()
        self.driver.quit()
            
    def run(self):
        """main function of the program"""        
        self.__scroll_and_scrape()

        for url in self.all_data:
            self.__insert_data_into_database(str(url), int(self.all_data[url][1])) 

        self.__exit_stage()

 


