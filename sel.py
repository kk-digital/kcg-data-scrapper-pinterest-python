import json
import undetected_chromedriver as uc
import os
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
import urllib
from proxy_checking import ProxyChecker

class Sel:
    def __init__(self,args):
        self.args = args
        
    def get_driver(self):
        user_data_dir = f"{os.getcwd()}/chrome"
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument("--window-size=1000,700")
        prefs = {"profile.managed_default_content_settings.images":2}
        options.add_experimental_option("prefs",prefs)
        if self.args["use_proxy"] == True:
            proxies = {}
            with open('proxies.json') as f:
                data = f.read()
                proxies = json.loads(data)
                print(f"proxies: {proxies}")
            proxy_to_use = None
            for address,port in proxies.items():
                proxy = f"{address}:{port}"
                if self.check_proxy_connection(proxy):
                    proxy_to_use = proxy
                    break
            if(proxy_to_use):
                options.add_argument('--proxy-server=%s' % proxy_to_use)
            else:
                print("No available proxy found.")
        #options.add_argument("--remote-debugging-port=9222")
        return uc.Chrome(options=options,user_data_dir=user_data_dir)
    
    def check_proxy_connection(self,proxy):
        try:
           checker = ProxyChecker()
           status = checker.check_proxy(proxy)
           if(status["status"] == 1):
                return True
           else:
                return False
                
        except IOError as e:
            print(e)
            return False
        else:
            print(f"using proxy: {proxy}")
            return True