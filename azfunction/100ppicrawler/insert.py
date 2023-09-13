import requests
import os
import datetime
import time
import pymongo
import pandas as pd
import lxml

from bs4 import BeautifulSoup
DCE_DB_NAME = "DCEdb"
DCE_COMMODITY_PRICE_COLLECTION_NAME = "CommodityPriceDb"


dce_future_info = {
'棕榈油': 'P',
'聚氯乙烯': 'V',
'聚乙烯': 'L',
'豆一':'A',
'豆粕':'M',
'豆油':'Y',
'玉米':'C',
'焦炭':'J',
'焦煤':'JM',
'铁矿石':'I',
'鸡蛋':'JD',
'聚丙烯':'PP',
'玉米淀粉':'CS',
'乙二醇':'EG',
'苯乙烯':'EB',
'液化石油气':'PG',
'生猪':'LH',
'PTA':'TA',
'菜籽油OI':'OI',
'油菜籽':'RS',
'菜籽粕':'RM',
'动力煤ZC':'ZC',
'强麦WH':'WH',
'粳稻':'JR',
'白糖':'SR',
'普麦':'PM',
'豆二':'B',
'纤维板':'FB',
'胶合板':'BB',
'粳米':'RR',
'PTA':'TA',
'棉花':'CF',
'早籼稻':'RI',
'甲醇MA':'MA',
'玻璃':'FG',
'晚籼稻':'LR',
'硅铁':'SF',
'锰硅':'SM',
'棉纱':'CY',
'鲜苹果':'AP',
'红枣':'CJ',
'尿素':'UR',
'纯碱':'SA',
'涤纶短纤':'PF',
'花生':'PK',
'菜籽油OI':'OI',
'工业硅':'SI',
'碳酸锂':'LC',
'铜':'CU',
'螺纹钢':'RB',
'锌':'ZN',
'铝':'AL',
'黄金':'AU',
'线材':'WR',
'燃料油':'FU',
'天然橡胶':'RU',
'铅':'PB',
'白银':'AG',
'石油沥青':'BU',
'热轧卷板':'HC',
'镍':'NI',
'锡':'SN',
'纸浆':'SP',
'不锈钢':'SS',
'丁二烯橡胶':'BR',
'原油':'SC',
'20号胶':'NR',
'低硫燃料油':'LU',
'国际铜':'BC',
'氧化铝':'AO',
'集运指数欧线期货':'EC',
}

market_list = ['上海期货交易所','大连商品交易所','郑州商品交易所','广州期货交易所']

COSMOS_CONNECTION_STRING = 'mongodb://lei:WMNi1Hh4vyOVG4V3OLcyaoZwOOl7YVHUUiKFeuGKbiS9rytxi2qd2VpnekCYj8IVrN6AFVHvLBUxACDb7vuFnA==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@lei@'
client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
DCEdb = client[DCE_DB_NAME]

DCE_commodity_price_collection = DCEdb[DCE_COMMODITY_PRICE_COLLECTION_NAME]


# sf_url = f'https://www.100ppi.com/sf/day-'
base_url='./sf'
html_files =[i for i in os.listdir('./sf') if i.endswith('html')]

for i in html_files:
    url = os.path.join(base_url,i)
    print(url)
    date_format = '%Y-%m-%d'
    date = datetime.datetime.strptime(url[-15:-5],date_format) 
    date =  date.date()
    with open(url,encoding="utf-8") as fp:
        sf_soup = BeautifulSoup(fp, 'html.parser')
        table = sf_soup.select('table#fdata')
        # print(table[0].prettify())
        a = pd.read_html(str(table))
        # logging.info(a[0])
        # logging.info(type(a[0]))
        # DCE_future_price_collection.insert_many(to_insert_items)
        market = ''
        many_commodity_price = []
        for index,row in a[0].iterrows():
            if row[0] in market_list:
                market = str(row[0])
            elif market == '':
                continue
            else:
                print(f'-----------')
                print(row[0])
                print(row[1])
                print(f'-----------')
                many_commodity_price.append({
                    'market':market,
                    'update_time':datetime.datetime.now(tz=datetime.timezone.utc),
                    'date':str(date),
                    'commodity_name': str(row[0]),
                    'commodity_code': dce_future_info[str(row[0])],
                    'commodity_price':float(row[1]),
                    'recent_future_code':dce_future_info[str(row[0])]+str(row[2]),
                    'recent_future_price':float(row[3]),
                    'recent_cf_basis':round(float(row[1])-float(row[3]),2),
                    'recent_cf_basis_percent':round((float(row[1])-float(row[3]))/float(row[1]),4),
                    'main_future_code':dce_future_info[str(row[0])]+str(row[5]),
                    'main_future_price':float(row[6]),
                    'main_cf_basis':round(float(row[1])-float(row[6]),2),
                    'main_cf_basis_percent':round((float(row[1])-float(row[6]))/float(row[1]),4),
                    'categoryID':None
                })
        # 数据库写入
        # print(many_commodity_price)
        print(many_commodity_price)
        if len(many_commodity_price) == 0:
            continue
        DCE_commodity_price_collection.insert_many(many_commodity_price)