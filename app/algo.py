import pymongo
import TT
from TT import MACD,RSI,KDJ
import pandas as pd
from datetime import datetime, timedelta, date
import time, os

# 数据库相关

import pymongo

DCE_DB_NAME = "DCEdb"
DCE_COMMODITY_PRICE_COLLECTION_NAME = "CommodityPriceDb"
DCE_FUTURE_PRICE_COLLECTION_NAME = "DCEFuturePriceDb"
DCE_FUTURE_CODE_COLLECTION_NAME = 'DCEFutureCodeDb'
DAILY_FUTURE_COLLECTION_NAME = "DailyFutureDb"
COSMOS_CONNECTION_STRING = 'mongodb://lei:WMNi1Hh4vyOVG4V3OLcyaoZwOOl7YVHUUiKFeuGKbiS9rytxi2qd2VpnekCYj8IVrN6AFVHvLBUxACDb7vuFnA==@lei.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@lei@'

client = pymongo.MongoClient(COSMOS_CONNECTION_STRING)
DCEdb = client[DCE_DB_NAME]

DCE_future_price_collection = DCEdb[DCE_FUTURE_PRICE_COLLECTION_NAME]
DCE_commodity_price_collection = DCEdb[DCE_COMMODITY_PRICE_COLLECTION_NAME]
DCE_future_code_collection = DCEdb[DCE_FUTURE_CODE_COLLECTION_NAME]
daily_future_collection = DCEdb[DAILY_FUTURE_COLLECTION_NAME]

def get_future_daily_price(start=None, end=None,code=[]):
	'''
	查询commodityprice表，获取某个商品或代码的历史现价，基差等
	'''
	if len(code)!=0:
		future_price_docs = DCE_commodity_price_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}},
								{'main_future_code':{'$in':code}}]}
							).sort("update_time", pymongo.ASCENDING)
	else:
		future_price_docs = DCE_commodity_price_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}}]}
							).sort("update_time", pymongo.ASCENDING)
	return future_price_docs

def get_future_price(start=None,end=None,code=[]):
	'''
	获取某个现货的相关信息
	'''
	if len(code)!=0:
		future_price_docs = DCE_future_price_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}},
								{'future_code':{'$in':code}}]}
							).sort("update_time", pymongo.ASCENDING)
	else:
		future_price_docs = DCE_future_price_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}}]}
							).sort("update_time", pymongo.ASCENDING)
	return future_price_docs


def get_previous_date(date_string, period=20, date_format='%Y-%m-%d'):
	"""Given a string date, return the date of given days before."""
	original_date = datetime.strptime(date_string, date_format)
	previous_date = original_date - timedelta(days=period)
	return previous_date.strftime(date_format)

def is_valid_date(date_string, date_format='%Y-%m-%d'):
	"""Check if the given string is a valid date."""
	try:
		datetime.strptime(date_string, date_format)
		return True
	except ValueError:
		return False

def generate_dates(start_date, end_date):
	"""Generate a list of dates between start_date and end_date (inclusive)."""
	start = datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)
	end = datetime.strptime(end_date, '%Y-%m-%d')

	# print(start,end)
	
	date_list = []
	delta = timedelta(days=1)
	while start <= end:
		date_list.append(start.strftime('%Y-%m-%d'))
		start += delta
	return date_list

def get_daily_price(start=None, end=None,code=[]):
	'''
	查询DailyFutureDb表，获取某个合约的open, close, high, low
	'''
	# daily_future_collection.create_index([("future_code", pymongo.ASCENDING),("date", pymongo.ASCENDING)])

	if len(code)!=0:
		daily_price_docs = daily_future_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}},
								{'future_code':{'$in':code}}]}
							)
							# ).sort([("future_code", pymongo.ASCENDING),("date", pymongo.ASCENDING)])
	else:
		daily_price_docs = daily_future_collection.find(
							{"$and":[
								{'date':{'$gte':start}},
								{'date':{'$lte':end}}]}
							)
							# ).sort([("future_code", pymongo.ASCENDING),("date", pymongo.ASCENDING)])

	return daily_price_docs

def get_realtime_price(date, codelist=[]):
	'''
	查询DCEFuturePriceDb表，获取某个合约的实时数据
	'''
	realtime_prices_df = pd.DataFrame()
	# print(codelist)
	for code in codelist:
		cur_realtime_prices = DCE_future_price_collection.find(
							{"$and":[
								{'date':date},
								{'future_code':code}]}
							).sort("clock", pymongo.DESCENDING).limit(1)

		cur_df = pd.DataFrame(list(cur_realtime_prices))
		# print(cur_df)
		realtime_prices_df = pd.concat([realtime_prices_df, cur_df], ignore_index=True)

	return realtime_prices_df

def quick_cursor2(code, end_date_string, min_count = 1500):
	t1 = time.time()
	prev_date_string = get_previous_date(end_date_string, period=5)

	minutes_cursors = DCE_future_price_collection.find(
					{"$and":[
						{'date':{'$gte':prev_date_string}},
						{'date':{'$lte':end_date_string}},
						{'future_code': code}]}
					)
	t2 = time.time()

	cur_df = pd.DataFrame(list(minutes_cursors))
	cur_df = cur_df.sort_values(by=['date','clock'], ascending=True).tail(1500)

	t3 = time.time()

	# print("===================== retry: ", retry)
	print(f"====代码运行时间: {t2 - t1} 秒")
	print(f"====代码运行时间: {t3 - t2} 秒")
	print("====",len(cur_df),"=====")

	return cur_df

def quick_cursor(code, end_date_string, df, min_count = 1500, retry = 1):
	if retry >0 and len(df) < min_count:
		t1 = time.time()

		minutes_cursors = DCE_future_price_collection.find(
						{"$and":[
							{'date':end_date_string},
							{'future_code': code}]}
						).sort("clock", pymongo.DESCENDING)

		cur_df = pd.DataFrame(list(minutes_cursors))
		t2 = time.time()

		if len(cur_df)> 0:
			df = pd.concat([df, cur_df], ignore_index=True)
		t3 = time.time()
		# print("===================== retry: ", retry)
		# print(f"====代码运行时间: {t2 - t1} 秒")
		# print(f"====代码运行时间: {t3 - t2} 秒")
		# print("====",len(df),"=====")

		prev_date_string = get_previous_date(end_date_string, period=1)
		return quick_cursor(code, prev_date_string, df, retry=retry-1)
	else:
		return df

def get_minutes_info(codes, end_date_string, min_count = 1500):
	minutes_df = pd.DataFrame()

	# DCE_future_price_collection.create_index([("date", pymongo.DESCENDING),("clock", pymongo.DESCENDING)])
	print(codes)
	for code in codes:
		print("cur code: ",code)
		cur_df = pd.DataFrame()
		# cur_df = quick_cursor2(code, end_date_string)
		cur_df = quick_cursor(code, end_date_string, cur_df, min_count = min_count, retry=5)

		if len(cur_df)>= min_count:
			t1= time.time()
			cur_df = cur_df.sort_values(by=['date','clock'], ascending=True)
			minutes_df = pd.concat([minutes_df, cur_df], ignore_index=True)
			t2 = time.time()
			# print(f"==*********==代码运行时间: {t2 - t1} 秒")
	
	return minutes_df

def get_daily_info(codes,dates_list , period, if_tail = False):
	# print(codes)
	prices = pd.DataFrame([i for i in get_daily_price(dates_list[0], dates_list[-1],codes)])
	# print(len(prices))

	if len(prices)>0:
		prices = prices.sort_values(by=["future_code","date"])
		
		# To conserve memory, tail the last period data
		if if_tail:
			# prices.to_csv("./prices_info.csv", index=False, encoding="gbk")
			new_df = pd.DataFrame()

			unique_codes = prices['future_code'].unique()
			for code in unique_codes:
				sub_df = prices[prices['future_code'] == code]
			
				last_p_rows = sub_df.sort_values(by='date', ascending=True).tail(period +1)
				new_df = pd.concat([new_df, last_p_rows], ignore_index=True)

			# print(len(new_df))

			return new_df
		else:
			return prices
	else: 
		raise ValueError(f"Insufficient data.")

def check_cross(line1, line2, m = 5):
	if len(line1) < m or len(line2) < m:
		raise ValueError(f"Both line1 and line2 should have at least {m} values.")

	for i in range(len(line1) - m + 1):
		subset_1 = line1[i:i+m]
		subset_2 = line2[i:i+m]

		# 查找初始金叉
		if subset_1[0] < subset_2[0] and subset_1[1] > subset_2[1]:
			trend = "gold"
			# 检查是否K始终大于D
			if all(k > d for k, d in zip(subset_1[1:], subset_2[1:])):
				return i+1, trend

		# 查找初始死叉
		elif subset_1[0] > subset_2[0] and subset_1[1] < subset_2[1]:
			trend = "dead"
			# 检查是否K始终小于D
			if all(k < d for k, d in zip(subset_1[1:], subset_2[1:])):
				return i+1, trend

	return None, None

def check_upper_ma(prices, realtime_prices, period, if_minute = False):
	unique_codes = prices['future_code'].unique()
	newest_prices = realtime_prices[["future_code", "newest_price"]]
	ma_selected_code = []
	for code in unique_codes:
		sub_df = prices[prices['future_code'] == code]
		if if_minute:
			close_prices = sub_df["newest_price"]
		else:
			close_prices = sub_df["close"]
		# print(close_prices)
		ma_value = TT.MA(close_prices, period)[-1]

		temp = newest_prices.loc[newest_prices["future_code"] == code,"newest_price"]
		if len(temp) >0:
			if ma_value < temp.item():
				ma_selected_code.append(code)	

	return ma_selected_code

def check_daily_kdj(daily_prices, codes, tor = 1):
	selected_code = []
	for code in codes:
		sub_df = daily_prices[daily_prices["future_code"] == code]
		close, high, low = sub_df['close'], sub_df['high'], sub_df['low']
		K, D, J = KDJ(close, high, low)

		# print("=================")
		# print(K)
		# print(D)
		if len(K) >= tor and len(D) >= tor:
			_, res = check_cross(K, D)

			if res == "gold":
				selected_code.append(code)

	return selected_code

def check_hourly_kdj(daily_prices, codes, tor = 2):
	
	daily_prices['hour'] = daily_prices['clock'] // 10000  # 将 230000 转换为 23，210000 转换为 21 等

	# 根据 future_code 和每小时进行分组，并聚合
	hourly_prices = daily_prices.groupby(['future_code', 'date', 'hour']).agg({
		'newest_price': ['last', 'max', 'min'],
		'future_code': 'first',
		'date': 'first',
		'clock': 'last'
	}).reset_index()

	# 调整列名
	hourly_prices.columns = ['future_code', 'date', 'hour', 'close', 'high', 'low', 'future_code_', 'date_', 'clock_']
	hourly_prices = hourly_prices[['future_code', 'date', 'clock_', 'close', 'high', 'low']]
	hourly_prices.columns = ['future_code', 'date', 'clock', 'close', 'high', 'low']

	hourly_prices = hourly_prices.sort_values(by=["future_code","date","clock"], ascending=[True, True, True])
	# hourly_prices.to_csv("./hourly_df.csv", index=False, encoding="gbk")	

	selected_code = []
	for code in codes:
		sub_df = hourly_prices[hourly_prices["future_code"] == code]
		close, high, low = sub_df['close'], sub_df['high'], sub_df['low']
		K, D, J = KDJ(close, high, low)

		if len(K) >= tor and len(D) >= tor:
			_, res = check_cross(K, D)

			if res == "gold":
				selected_code.append(code)

	return selected_code

"""
	Here, 'end_date_string' typically represents today. 
	However, 'end_date' should be adjusted to be one day ahead ('end_date_ahead') for easier calculations, 
	especially when computing indicators such as MA and KDJ.
"""
def get_name_list(end_date_string, period = 20, period_minute = 20, threshold = 0.05):
	end_date = datetime.strptime(end_date_string, "%Y-%m-%d")
	end_date_ahead = end_date - timedelta(days=1)
	end_date_ahead_string = end_date_ahead.strftime('%Y-%m-%d')  # 格式为 YYYY-MM-DD

	if is_valid_date(end_date_string):

		"""
			C1: 正基差过滤
		"""
		t1 = time.time()
		# get namelist by basis of percentage
		print(end_date_ahead_string)
		basis_future = pd.DataFrame([i for i in DCE_commodity_price_collection.find(
						# {"date": end_date_string}
						{"date": end_date_ahead_string}
					).sort("main_future_code", pymongo.ASCENDING) ])
		# print(basis_future)
		print(basis_future)
		basis_future = basis_future.sort_values(by='main_cf_basis_percent', ascending=False)

		t2 = time.time()

		# Find the main contract with +5% positive basis 
		codes = basis_future[basis_future["main_cf_basis_percent"]>=threshold]["main_future_code"].tolist()
		codes = list(set(codes))

		"""
			C2: 价格高于20日均线，且日KDJ交金叉
		"""
		# A larger date range (2*period) to ensure there's no data shortage 
		# due to non-trading days on weekends (Saturday and Sunday).
		start_date_string = get_previous_date(date_string = end_date_string, period=period*2+1) 
		dates_list = generate_dates(start_date_string, end_date_string)

		# get the price information of main contract, including high,low, open,close
		daily_prices = get_daily_info(codes, dates_list, period) 
		# print(daily_prices.columns) #['_id', 'date', 'future_code', 'close', 'open', 'high', 'low', 'deal', 'categoryID'],
		
		t3 = time.time()
		# print(f'xxxxx')
		# print(end_date_string)
		realtime_prices = get_realtime_price(end_date_string, codes)
		# realtime_prices.to_csv("./realtime_prices.csv", index=False, encoding="gbk")

		ma_selected_codes = check_upper_ma(daily_prices, realtime_prices, period)
		# print(ma_selected_codes)

		t4 = time.time()

		kdj_selected_codes = check_daily_kdj(daily_prices, ma_selected_codes)
		# kdj_selected_codes = check_daily_kdj(daily_prices, codes)
		# print(kdj_selected_codes)

		"""
			C3: 价格高于60分钟-20均线，且60分钟-KDJ交金叉
		"""
		t5 = time.time()

		# for fast verification
		if os.path.isfile("./minutes_df.csv"):
			minutes_df = pd.read_csv("./minutes_df.csv", encoding="gbk")
		else:
			minutes_df = get_minutes_info(codes, end_date_string)
			# minutes_df.to_csv("./minutes_df.csv", index=False, encoding="gbk")	
			print(len(minutes_df))

		t6 = time.time()

		hourly_prices = minutes_df.groupby('future_code').apply(\
			lambda group: group.iloc[::-60]).reset_index(drop=True)

		ma_selected_codes_hourly = check_upper_ma(hourly_prices, realtime_prices, period_minute, if_minute=True)
		# print(ma_selected_codes_hourly)

		kdj_selected_codes_hourly = check_hourly_kdj(minutes_df, ma_selected_codes_hourly)
		# print(kdj_selected_codes_hourly)
		t7 = time.time()

		# 创建空的 DataFrame
		final_codes = list(set(kdj_selected_codes) & set(kdj_selected_codes_hourly))
		final_results = pd.DataFrame(columns=['评估指标','评估结果'])
		final_results.loc[0] = ["正基差", codes]
		final_results.loc[1] = ["价格高于20日均线，且日KDJ交金叉", kdj_selected_codes]
		final_results.loc[2] = ["价格高于60分钟-20均线，且60分钟-KDJ交金叉", kdj_selected_codes]
		final_results.loc[3] = ["综合结果", final_codes]
		final_results.to_csv("./final_results.csv", index=False, encoding="gbk")

		# print(f"代码运行时间: {t2 - t1} 秒")
		# print(f"代码运行时间: {t3 - t2} 秒")
		# print(f"代码运行时间: {t4 - t3} 秒")
		# print(f"代码运行时间: {t5 - t4} 秒")
		# print(f"代码运行时间: {t6 - t5} 秒")
		# print(f"代码运行时间: {t7 - t6} 秒")
		# print("正基差： ",codes)
		# print("ma_selected_codes ",ma_selected_codes)
		# print("kdj_selected_codes ",kdj_selected_codes)
		# print("ma_selected_codes_hourly ",ma_selected_codes_hourly)
		# print("kdj_selected_codes_hourly ",kdj_selected_codes_hourly)
		# print("final_codes ", final_codes)
		# print(final_results)
		print(final_results)
		return final_results
	else:
		print("错误日期信息")
		return None


def main():
	# 获取今天的日期
	today_date = date.today()
	today_string = today_date.strftime('%Y-%m-%d')  # 格式为 YYYY-MM-DD
	# print(today_string)
	# To check：周一六日对应的日期会报错，在查期货实时价格数据库的时候，没有假日对应的信息，查现货价格时假日延后一天的信息
	today_string = "2023-09-15" 
	results = get_name_list(end_date_string = today_string, period = 20, threshold = 0.05)
	return results
if __name__ == "__main__":
	main()
