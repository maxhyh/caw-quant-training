from binance.client import Client
from datetime import datetime
import os
import pandas as pd

class binance_data():

    def __init__(self):
        self.client = Client("","")

    def get_order_book(self, symbol):
        return self.client.get_order_book(symbol = symbol)

    def get_historical_klines(self, symbol, interval, start_str, end_str):
        return self.client.get_historical_klines(symbol, interval, start_str, end_str)

    def get_transactions(self, symbol):
        return self.client.get_recent_trades(symbol=symbol)

DATA_DIR = '/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task2/'

bd = binance_data()

data_MD = bd.get_order_book("ETHBTC")
df1 = pd.DataFrame(data_MD)
df1.to_csv(os.path.join(DATA_DIR, "market_depth.csv"), index=False)

data_his = bd.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_1WEEK, "1 Apr, 2019", "1 Apr, 2020")
df2 = pd.DataFrame(data_his)
df2.to_csv(os.path.join(DATA_DIR, "historical_kline.csv"), index=False)

data_trade = bd.get_transactions("ETHBTC")
df3 = pd.DataFrame(data_trade)
df3.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)
