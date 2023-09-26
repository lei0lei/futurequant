import datetime
import logging
import azure.functions as func

import requests
import os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import trange
import pymongo

# import os

# proxy = 'https://localhost:7890'
# os.environ['http_proxy'] = proxy 
# os.environ['HTTP_PROXY'] = proxy
# os.environ['https_proxy'] = proxy
# os.environ['HTTPS_PROXY'] = proxy

DCE_DB_NAME = "DCEdb"
DCE_FUTURE_CODE = "DCEFutureCodeDb"
DAILY_FUTURE_DB = 'DailyFutureDb'

COSMOS_CONNECTION_STRING = 'mongodb://lei:jDa0YcHR5WpGRQUaCsK5ZzObqJpjbiEwYF4S8FfbbWWMkqV1HTtVzWFkF8U0lqRXRbLZfVJtyIYoACDbqnzU3w==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@lei@'

client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
DCEdb = client[DCE_DB_NAME]

date =  datetime.date.today()-datetime.timedelta(days=1)

app = func.FunctionApp()

start = '1978-01-01'
end = '2025-01-01'
# 需要其他期货数据，修改此处代码

allpage = 20


# ---------------------------------------------------
# @app.schedule(schedule="0 0 8 * * 2-6", arg_name="myTimer", run_on_startup=True,
#               use_monitor=False) 
# def DailyFutureCrawler(myTimer: func.TimerRequest) -> None:
#     utc_timestamp = datetime.datetime.utcnow().replace(
#         tzinfo=datetime.timezone.utc).isoformat()

#     if myTimer.past_due:
#         logging.info('The timer is past due!')

#     logging.info('Python timer trigger function ran at %s', utc_timestamp)
# -----------------------------------------

def run():
    # codes = get_future_code()
    DCE_future_daily_collection = DCEdb[DAILY_FUTURE_DB]
    # codes = get_future_code()
    codes = [ 
          'AG2310',
      'AG2311', 'AG2312', 'AG2401', 'AG2402', 'AG2403', 'AG2404', 'AG2405',
      'AG2406', 'AG2407', 'AG2408', 'AG2409', 'BU0',    'BU2310', 'BU2311',
      'BU2312', 'BU2401', 'BU2402', 'BU2403', 'BU2404', 'BU2405', 'BU2406',
      'BU2407', 'BU2409', 'BU2412', 'BU2503', 'BU2506', 'BU2509', 'HC0',
      'HC2310', 'HC2311', 'HC2312', 'HC2401', 'HC2402', 'HC2403', 'HC2404',
      'HC2405', 'HC2406', 'HC2407', 'HC2408', 'HC2409', 'NI0',    'NI2310',
      'NI2311', 'NI2312', 'NI2401', 'NI2402', 'NI2403', 'NI2404', 'NI2405',
      'NI2406', 'NI2407', 'NI2409', 'SN0',    'SN2310', 'SN2311', 'SN2312',
      'SN2401', 'SN2402', 'SN2403', 'SN2404', 'SN2405', 'SN2406', 'SN2407',
      'SP0',    'SP2310', 'SP2311', 'SP2312', 'SP2401', 'SP2402', 'SP2403',
      'SP2404', 'SP2405', 'SP2406', 'SP2407', 'SP2408', 'SP2409', 'SS0',
      'SS2310', 'SS2311', 'SS2312', 'SS2401', 'SS2402', 'SS2403', 'SS2404',
      'SS2405', 'SS2406', 'SS2407', 'SS2408', 'BR0',    'BR2401', 'BR2402',
      'BR2403', 'BR2404', 'BR2405', 'BR2406', 'BR2407', 'BR2408', 'BR2409',
      'SC0',    'SC2310', 'SC2311', 'SC2312', 'SC2401', 'SC2402', 'SC2403',
      'SC2406', 'SC2412', 'SC2609', 'NR0',    'NR2310', 'NR2311', 'NR2312',
      'NR2401', 'NR2402', 'NR2403', 'NR2405', 'NR2406', 'LU0',    'LU2310',
      'LU2311', 'LU2312', 'LU2401', 'LU2402', 'LU2405', 'BC0',    'BC2310',
      'BC2311', 'BC2312', 'BC2401', 'AO0',    'AO2311', 'AO2312', 'AO2401',
      'AO2402', 'AO2403', 'AO2404', 'AO2405', 'AO2406', 'AO2407', 'AO2408',
      'AO2409', 'EC0',    'EC2404', 'EC2406', 'EC2408', 'EC2410', 'EC2412']
    for code in codes:
        blanklist = []
        for p in range(allpage):
            print(p)
            page_url = f'https://vip.stock.finance.sina.com.cn/q/view/vFutures_History.php?page='+str(p+1)+'&breed='+code+'&type=inner&start='+start+'&end='+end
            # page_url = f'https://vip.stock.finance.sina.com.cn/q/view/vFutures_History.php?page=1&breed=C2111&type=inner&start=20230920&end=20230920'
            print(page_url)
            r = requests.get(page_url)
            soup = BeautifulSoup(r.text.encode('utf8'), 'html.parser')
            table = soup.find_all("div", class_="historyList")
            if len(table)<1:
                continue
            table = BeautifulSoup(table[0].prettify().encode('utf8'), 'html.parser')
            a = pd.read_html(table.prettify())
            if a[0].shape[0]<2:
                break
            # print(a[0])
            # print(a[0].shape[0])
            for index,row in a[0].iterrows():
                # print(row)
                if index>=1:
                    if DCE_future_daily_collection.find_one({'$and':[{'date':row[0]},{'future_code':code}]}):
                        continue

                    else:
                        blanklist.append({'date':row[0],
                                        'future_code':code,
                                        'close':float(row[1]),
                                        'open':float(row[2]),
                                        'high':float(row[3]),
                                        'low':float(row[4]),
                                        'deal':int(row[5]),
                                        'categoryID':None})
        if len(blanklist)==0:
            continue
        DCE_future_daily_collection.insert_many(blanklist)
        logging.info(f'{code} daily info add to db DailyFutureDb')
def get_future_code():
    DCE_future_code_collection = DCEdb[DCE_FUTURE_CODE]
    return DCE_future_code_collection.find({}).sort('update_time',pymongo.DESCENDING)[0]['future_code']
    

if __name__ == '__main__':
    run()