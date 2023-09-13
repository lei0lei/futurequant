import requests
import os
import datetime
import time
# import pymongo
# import pandas as pd
# import lxml

# from bs4 import BeautifulSoup


# 现期表
sf_url = f'https://www.100ppi.com/sf/day-'
sf_save_dir = './sf'
# 基差表 
sf2_url = f'https://www.100ppi.com/sf2/day-'
sf2_save_dir = './sf2'

proxy_servers = {
   'http': '127.0.0.1:7890',
   'https': '127.0.0.1:7890',
}


start = datetime.date(2015,2,9)
end = datetime.date(2015,5,11)

while end>=start:
    print(str(start))
    sf_complete_url = sf_url+str(start)+'.html'
    
    sf2_complete_url = sf2_url+str(start)+'.html'
    r = requests.get(sf_complete_url,proxies=proxy_servers)
    # time.sleep(1)
    content = r.text
    _r = requests.get(sf2_complete_url,proxies=proxy_servers)
    # time.sleep(1)
    _content = _r.text
    with open(os.path.join(sf_save_dir,'sf-day-'+str(start)+'.html'), 'w',encoding="utf-8") as f:
        f.write(content)
    with open(os.path.join(sf2_save_dir,'sf2-day-'+str(start)+'.html'), 'w',encoding="utf-8") as f:
        f.write(_content)

    start += datetime.timedelta(days=1)

