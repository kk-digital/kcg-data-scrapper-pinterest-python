import requests
from bs4 import BeautifulSoup
from helper_functions import init_driver
def main():
    driver = init_driver()
    driver.get('https://www.pinterest.com/pixbrix/pok%C3%A9mon-pixel-art/')    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    for div_tag in soup.find_all("div"):
        try:
            if div_tag["data-test-id"] == "board-section":
                return True
        except Exception as e:
            continue    
    return False



if __name__ == '__main__':
    print(main())