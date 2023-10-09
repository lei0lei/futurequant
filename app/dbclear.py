import pymongo

DCE_DB_NAME = "DCEdb"
DCE_COMMODITY_PRICE_COLLECTION_NAME = "CommodityPriceDb"
DCE_FUTURE_PRICE_COLLECTION_NAME = "DCEFuturePriceDb"
DCE_FUTURE_CODE_COLLECTION_NAME = 'DCEFutureCodeDb'
COSMOS_CONNECTION_STRING = 'mongodb://lei:WMNi1Hh4vyOVG4V3OLcyaoZwOOl7YVHUUiKFeuGKbiS9rytxi2qd2VpnekCYj8IVrN6AFVHvLBUxACDb7vuFnA==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=600000&appName=@lei@'


client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
DCEdb = client[DCE_DB_NAME]
DCE_future_price_collection = DCEdb[DCE_FUTURE_PRICE_COLLECTION_NAME]
DCE_commodity_price_collection = DCEdb[DCE_COMMODITY_PRICE_COLLECTION_NAME]

import datetime
base = datetime.date.today()
numdays = 12
dates_list = [str(base - datetime.timedelta(days=x)) for x in range(numdays)]


from tqdm import tqdm
for to_delete_date in reversed(dates_list):
    print(f'clearing date {to_delete_date}')
    for i in tqdm(range(800)):
        print(f'{i}/800')
        pipeline = [
        {"$match":{"date":{"$eq":to_delete_date}}}, 
        { "$group": { "_id": { "future_code": "$future_code", "clock": "$clock", "date": "$date" }, 
                    "_idsNeedsToBeDeleted": { "$push": "$$ROOT._id" } 
                    } }, 
        { "$project": { "_id": 0, 
                    "_idsNeedsToBeDeleted": { "$slice": ["$_idsNeedsToBeDeleted", 1, { "$size": "$_idsNeedsToBeDeleted" }] } } },
        { "$unwind": "$_idsNeedsToBeDeleted"  },
        {"$limit":100},
        { "$group": { "_id": "", "_idsNeedsToBeDeleted": { "$push": "$_idsNeedsToBeDeleted" } } }, 
        { "$project": { "_id": 0 } },
        ]
        duped_items = list(DCEdb.DCEFuturePriceDb.aggregate(pipeline))
        if len(duped_items) ==0:
            break
        to_delete_items = duped_items[0]['_idsNeedsToBeDeleted']
        # print(len(to_delete_items))
        if len(to_delete_items) > 0:
            DCE_future_price_collection.delete_many( { "_id" : {"$in" : to_delete_items} } )
        else:
            break

