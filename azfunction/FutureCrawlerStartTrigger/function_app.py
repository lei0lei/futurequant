import datetime
import logging
import azure.functions as func

import time
import requests

import os
import sys
from random import randint
import pandas as pd
import pymongo
# from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List

DCE_DB_NAME = "DCEdb"
DCE_FUTURE_PRICE_COLLECTION_NAME = "DCEFuturePriceDb"

DCE_FUTURE_CODE_COLLECTION_NAME = 'DCEFutureCodeDb'


@dataclass
class Future:
    future_code:str
    future_prefix: bool


COSMOS_CONNECTION_STRING = 'mongodb://lei:WMNi1Hh4vyOVG4V3OLcyaoZwOOl7YVHUUiKFeuGKbiS9rytxi2qd2VpnekCYj8IVrN6AFVHvLBUxACDb7vuFnA==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@lei@'
app = func.FunctionApp()

@app.schedule(schedule="0 */1 * * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def FutureCrawlerStartTrigger(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
    DCEdb = client[DCE_DB_NAME]
    if DCE_DB_NAME not in client.list_database_names():
        # Create a database with 400 RU throughput that can be shared across
        # the DB's collections
        
        logging.info(f"no db:{DCE_DB_NAME}")
    else:
        logging.info(f"found db:{DCE_DB_NAME}")
        DCE_future_price_collection = DCEdb[DCE_FUTURE_PRICE_COLLECTION_NAME]
        DCE_future_code_collection = DCEdb[DCE_FUTURE_CODE_COLLECTION_NAME]

    doc = DCE_future_code_collection.find({}).sort("update_time", pymongo.DESCENDING)[0]
    all_future_code = doc['future_code']
    Referer = 'https://finance.sina.com.cn/futures/quotes.shtml'
    Host = 'hq.sinajs.cn'
    code_prefix = 'nf_'
    code_str = ''
    for k in all_future_code:
        code_str+=code_prefix
        code_str+=k
        code_str+=','

    query_str = 'https://hq.sinajs.cn/?_='+str(int(time.time()*1000))+'/$list='+code_str

    response = requests.get(query_str,headers={'Referer':Referer,'Host':Host})
    t = response.text.splitlines()
    ft = [i.split('_')[-1] for i in t]
    # print(ft)
    to_insert_items = []
    for i in ft:
        to_insert_item = {}
        to_insert_item['categoryID'] = None
        to_insert_item['market'] = 'DCE'
        to_insert_item['future_code'] = i.split('=')[0]
        # print(to_insert_item['future_code'])
        _items = i.split('"')[1]
        # print(_items)
        items = _items.split(',')
        # print(items)
        # print(len(items))
        if len(items)==1:
            continue
        to_insert_item['future_name'] = items[0]
        print(items[0])
        to_insert_item['clock'] = int(items[1])
        to_insert_item['date'] = items[17]
        to_insert_item['open_price'] = float(items[2])
        to_insert_item['max_price'] = float(items[3])
        to_insert_item['min_price'] = float(items[4])
        to_insert_item['close_price'] = float(items[5])
        to_insert_item['yesterday_price'] = float(items[10])
        to_insert_item['buy_price'] = float(items[6])
        to_insert_item['sell_price'] = float(items[7])
        to_insert_item['newest_price'] = float(items[8])
        to_insert_item['buy_amount'] = float(items[11])
        to_insert_item['sell_amount'] = float(items[12])
        to_insert_item['amount'] = float(items[14])
        to_insert_item['volume'] = float(items[13])
        to_insert_item['everage_price'] = float(items[27])
        to_insert_items.append(to_insert_item)
        del to_insert_item
    # print(to_insert_items)
    DCE_future_price_collection.insert_many(to_insert_items)
    logging.info(f'write to database:{datetime.datetime.now()}')