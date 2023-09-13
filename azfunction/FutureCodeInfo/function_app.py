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
DCE_COMMODITY_CODE_COLLECTION_NAME = "DCECommodityCodeDb"

DCE_FUTURE_CODE_COLLECTION_NAME = 'DCEFutureCodeDb'


@dataclass
class CommoditySpider:
    spider_url:str
    spidered: bool
    commodity_code:str
    future_code: List[str]

# @app.cosmos_db_output(arg_name="outputDocument",
#                     database_name="DCEdb", 
#                     connection=None,
#                     container_name=None,
#                     collection_name="DCECommodityCodeDb", 
#                     connection_string_setting="CosmosDbConnectionString")

COSMOS_CONNECTION_STRING = 'mongodb://lei:WMNi1Hh4vyOVG4V3OLcyaoZwOOl7YVHUUiKFeuGKbiS9rytxi2qd2VpnekCYj8IVrN6AFVHvLBUxACDb7vuFnA==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@lei@'
app = func.FunctionApp()

def get_all_future_url(collection):
    query_url = []
    for doc in collection.find({}).sort("id", pymongo.ASCENDING):
        # {'_id': ObjectId('64fd6128593e9b115c9d3025'), 
        # 'id': 17, 
        # 'commodity_name': '粳米', 
        # 'commodity_code': 'RR', 
        # 'commodity_search_base_url': 'https://money.finance.sina.com.cn/mkt/', 
        # 'commodity_search_surfix_url': '#gm_qh', 
        # 'categoryID': None}
        print(doc)
        query_url.append(doc['commodity_search_base_url']+doc['commodity_search_surfix_url'])
    return query_url


def get_all_future_code_and_info(urls):
    pass


def insert_to_DCE_future_code(collection,info):
    pass

# 0 */1 * * * 1-5
@app.schedule(schedule="0 0 */1 * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
# @app.cosmos_db_input(arg_name="documents", 
#                      database_name="DCEdb",
#                      container_name="DCECommodityCodeDb",
#                      id="1",
#                      partition_key="categoryID",
#                      connection="CosmosDbConnectionString",
#                      )
def FutureCodeInfo(myTimer: func.TimerRequest,) -> None:
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
        DCE_commodity_code_collection = DCEdb[DCE_COMMODITY_CODE_COLLECTION_NAME]
        DCE_future_code_collection = DCEdb[DCE_FUTURE_CODE_COLLECTION_NAME]
    print(f'1')
    # 获取DCE 商品基础url
    DCE_commodity_query_urls = get_all_future_url(DCE_commodity_code_collection)

    # 爬取商品代码及基础数据

    commoditys = []
    results = []
    print(DCE_commodity_query_urls)
    for url in DCE_commodity_query_urls:
        api_part = url.split('#')[-1]
        print(api_part)
        api_path = f'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQFuturesData?page=1&num=80&sort=symbol&asc=1&node={api_part}&_s_r_a=init'

        commodity = CommoditySpider(api_path,False,api_part,None)

        result = requests.get(api_path).json()
        if len(result)>0:
            commodity.spidered = True
            commodity.future_code =[i['symbol'] for i in result]
        commoditys.append(commodity)
    
    for k in commoditys:
        if k.spidered is False:
            result = requests.get(k.spider_url).json()
            if len(result)>0:
                commodity.spidered = True
                commodity.future_code =[i['symbol'] for i in result]

    print(commoditys)

    # 插入商品代码 
    tt=int(time.time())
    future_code = []
    for j in commoditys:
        future_code+=j.future_code
    to_insert_dce = {'update_time':tt,
                    'market':'DCE',
                    'future_code': future_code,
                    'categoryID':None}

    print(to_insert_dce)
    DCE_future_code_collection.insert_one(to_insert_dce)


        # print(result)
        # print(result[0])
        # soup = BeautifulSoup(response.text.encode('utf8'), 'html.parser')
        # soup = get_dynamic_soup(url)
        # print(soup.prettify())
        # table = soup.select('div#tbl_wrap')
        # print(f'-----------')
        # print(table[0].get_text())
        # print(table[0].prettify())

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)