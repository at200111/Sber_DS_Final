import requests 
import json
import cloudscraper
import random 
import time
from threading import Timer 

class Proxies:
    def __init__(self):
        self.VALID_STATUSES = [200, 301, 302, 307, 404]
        # two types of sessions, because cloudscraper doesn't allow verify=False and doesnt accespt self signed certificates, used in zenrows.com
        self.requests_session = requests.Session() 
        self.cloudscraper_session = cloudscraper.create_scraper()

        self.proxies_list = set()
        self.unchecked = set() 
        self.working = set() 
        self.not_working = set()
        self.proxies_stats = dict() 
        
        self.reload_timer_active = True
        self.reset_timer_active = True

        self.reload_proxies()

    def reload_proxies(self):
        if self.proxies_stats:
            with open('./src/proxies_stats.txt', 'w') as f:
                json.dump(dict(sorted(self.proxies_stats.items(), key=lambda item: item[1])), f, ensure_ascii=False)

        load_proxies_list = set(open("./src/proxies.txt", "r").read().strip().split("\n"))
        new_proxies_list = load_proxies_list.difference(self.proxies_list)
        if new_proxies_list:
            self.unchecked.update(new_proxies_list)
            self.proxies_list = load_proxies_list

        # check new proxies every n sec
        if self.reload_timer_active:
            Timer(60.0, self.reload_proxies).start() 
 
    def reset_proxy(self, proxy): 
        self.unchecked.add(proxy) 
        self.working.discard(proxy) 
        self.not_working.discard(proxy) 
 
    def set_working(self, proxy): 
        self.unchecked.discard(proxy) 
        self.working.add(proxy) 
        self.not_working.discard(proxy) 
        self.proxies_stats[proxy] = self.proxies_stats.get(proxy, 0) + 1
  
    def set_not_working(self, proxy): 
        self.unchecked.discard(proxy) 
        self.working.discard(proxy) 
        self.not_working.add(proxy)
        self.proxies_stats[proxy] = self.proxies_stats.get(proxy, 0) - 1
        # move to unchecked after a certain time (20s)
        # excluded from list after 5 unsuccessfull attempts
        if self.proxies_stats.get(proxy, 0) > -5 and self.reset_timer_active:            
            Timer(30.0, self.reset_proxy, [proxy]).start()  

    def cancel_timers(self):        
        self.reload_timer_active = False
        self.reset_timer_active = False
   
    def get_random_proxy(self): 
        # create a tuple from unchecked and working sets 
        available_proxies = tuple(self.unchecked.union(self.working)) 
        if not available_proxies: 
            #raise Exception("no proxies available") 
            print('No proxies available. Waiting 30 sec')
            time.sleep(30)
        else:
            return random.choice(available_proxies)
    

    def get(self, url, params=None, proxy = None, debug = False, verbose = 0): 
        random_proxy = not proxy

        for _ in range(0, 10):
            if random_proxy:
                proxy = self.get_random_proxy() 
    
            try: 
                if verbose == 1:
                    print(proxy, end=' ')
                try: 
                    response = self.cloudscraper_session.get(url, proxies={'http': f"http://{proxy}", 'https': f"http://{proxy}",}, params=params, timeout=15) 
                except requests.exceptions.SSLError:
                    response = self.requests_session.get(url, proxies={'http': f"http://{proxy}", 'https': f"http://{proxy}",}, params=params, timeout=15, verify=False)

                if response.status_code in self.VALID_STATUSES: 
                    self.set_working(proxy)
                    if verbose == 1:
                        print('is working')
                    return response
                else: 
                    self.set_not_working(proxy) 
                    if verbose == 1:
                        print('is not working')
                    if not random_proxy:
                        break
                    
            except Exception as e: 
                if verbose == 1:
                    print(e)
                if debug:
                    raise e 

                self.set_not_working(proxy)         
                if not random_proxy:
                    break
        
    
    def check_proxies(self, verbose = 0): 
        for proxy in list(self.unchecked): 
            self.get("http://ident.me/", proxy = proxy, verbose = verbose)