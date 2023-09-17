from proxies import Proxies
from bs4 import BeautifulSoup
import tqdm
import json
import re
import warnings 
from pathlib import Path
from urllib3.exceptions import InsecureRequestWarning


PARAM_TOTIME = {'month':2592000, 'week':604800, 'today':-2, 'hour':3600, 'all':0}

class OfferScraper:
    def __init__(self):
        self.BASE_URL = 'https://www.cian.ru/cat.php'
        # парамеры поиска
        self.params = {
            'deal_type': 'sale',
            'engine_version': '2',
            'mintarea': '30',
            'maxtarea': '500',            
            'offer_type': 'flat',
            'p': '1',
            'region': '1',    
        }
        self.proxy = Proxies()
        self.progress_bar = tqdm.tqdm()
        warnings.simplefilter('ignore', InsecureRequestWarning)
        Path('./data/process/').mkdir(parents=True, exist_ok=True)

    def get_json_toc(self, params):        
        with self.proxy.get(self.BASE_URL, params=params) as request: 
            try:
                root_data = BeautifulSoup(request.content, 'lxml')
                head_data = root_data.find('head')
                tag_data = head_data.find('script', string = re.compile('ca\("pageview'))
                
                # может вернуться или pageview или pageviewSite
                try:
                    return re.search(r'ca\("pageview",(.*?)\)', tag_data.text).group(1)
                except:
                    try:
                        return re.search(r'ca\("pageviewSite",(.*?)\)', tag_data.text).group(1)
                    except Exception as e: 
                        self.progress_bar.write(str(e))                       
            except Exception as e: 
                self.progress_bar.write('Parse TOC:' + str(e))

    def get_json_page(self, offer_id):            
        with self.proxy.get('https://www.cian.ru/sale/flat/' + offer_id) as request: 
            try:                
                root_data = BeautifulSoup(request.content, 'lxml')                                
                tag_data = root_data.find('script', string = re.compile('offerData'))                            
                raw_contents = re.search(r'concat\((.*?)\);\n', tag_data.text).group(1)
        
                if raw_contents:
                    return json.loads(raw_contents)                
            except Exception as e: 
                self.progress_bar.write('Parse page: ' + str(e))

    def full_scrap(self, search_depth = 'day', verbose = 0):
        mintarea = int(self.params.get('mintarea', 0))
        maxtarea = int(self.params.get('maxtarea', 9999))
        areas = list(zip(
                    [max(0, mintarea)] + list(range(max(15, mintarea), min(maxtarea, 250))) + [min(maxtarea, 251)], 
                    [max(14, mintarea)] + list(range(max(15, mintarea), min(maxtarea, 250))) + [min(maxtarea, 9999)]))        
        minfloor = int(self.params.get('minfloor', 1))
        maxfloor = int(self.params.get('maxfloor', 200))
        floors = list(zip(
                    [max(0, minfloor)] + list(range(max(15, minfloor), min(maxfloor, 250))) + [min(maxfloor, 251)], 
                    [max(14, minfloor)] + list(range(max(15, minfloor), min(maxfloor, 250))) + [min(maxfloor, 9999)]))            
        
        # just not to make two fors. if nessesary add more terms
        iter_list = [[a, f] for a in areas for f in floors]

        # init proxies
        if verbose == 1:
            print('init proxies')

        self.proxy.check_proxies(verbose = verbose)

        if verbose == 1:
            print('init proxies finished')
        
        params = self.params.copy()          
        params['totime'] = PARAM_TOTIME.get(search_depth, 0)                
        
        self.progress_bar.total = total = len(iter_list) * 55
        for area, floor in iter_list:            
            params['mintarea'] = area[0]
            params['maxtarea'] = area[1]
            params['minfloor'] = floor[0]
            params['maxfloor'] = floor[1]
            
            for page in range(1, 55):
                self.progress_bar.update(1)
                params['p'] = page
                # parse search result to get links to offers
                json_toc = self.get_json_toc(params)                
                if json_toc:                                      
                    current_page = json.loads(json_toc).get('page').get('pageNumber')                    
                    # when "p" param is invalid we are redirected to the first page
                    if page > 1 and current_page == 1:
                        break
                    try:                           
                        for item in json.loads(json_toc).get('products'):                             
                            offer_id = str(item.get('id'))                            
                            json_page = self.get_json_page(offer_id)
                            if json_page:                                                                
                                with open('./data/process/' + offer_id + '.json', 'w', encoding="utf-8") as f:
                                    json.dump(json_page, f, ensure_ascii=False)                                              
                        
                    except Exception as e: 
                        self.progress_bar.write(str(e))

            self.progress_bar.update(55 - page)
        

    def finish_scrap(self):
        self.proxy.cancel_timers()
    
if __name__ == '__main__':        
    scraper = OfferScraper()
    scraper.full_scrap(verbose=0)

    scraper.finish_scrap()



