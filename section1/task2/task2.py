from binance.client import Client
from datetime import datetime
import os
import pandas as pd
from binance.websockets import BinanceSocketManager

class binance_data():

    def __init__(self):
        self.client = Client("","")

    def get_order_book(self):

        return self.client.get_orderbook_tickers()


    def get_historical_klines(self, symbol, interval, start_str, end_str):
        return self.client.get_historical_klines(symbol, interval, start_str, end_str)

    def get_transactions(self, symbol):
        if isinstance(symbol,str):
            return self.client.get_recent_trades(symbol=symbol)
        else:
            raise Exception("Symbol should be an instance of string.")

def unix2date(timestamp):
    date = int(timestamp)/1000
    return datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')


DATA_DIR = '/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task2/'

bd = binance_data()


data_MD = bd.get_order_book()
df1 = pd.DataFrame(data_MD)
df1.to_csv(os.path.join(DATA_DIR, "market_depth.csv"), index=False)

data_his = bd.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_1WEEK, "1 Apr, 2019", "1 Apr, 2020")
df2 = pd.DataFrame(data_his, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',\
                                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', \
                                   'taker_buy_quote_asset_volume', 'ignored'])
df2['open_time'] = df2['open_time'].apply(lambda x : unix2date(x))
df2['close_time'] = df2['close_time'].apply(lambda x : unix2date(x))
df2.to_csv(os.path.join(DATA_DIR, "historical_kline.csv"), index=False)

data_trade = bd.get_transactions("ETHBTC")
df3 = pd.DataFrame(data_trade)
df3.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)
