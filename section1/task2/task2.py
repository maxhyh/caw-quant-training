from binance.client import Client
from datetime import datetime
import os
import pandas as pd
import numpy as np
from binance.websockets import BinanceSocketManager

class binance_data():

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(api_key,api_secret)

    def get_order_book(self, symbol):

        order_book = self.client.get_order_book(symbol = symbol)

        bids = pd.DataFrame.from_dict(order_book["bids"]).iloc[:, 0:2]
        bids.columns = ['bid_price', 'bid_qty']

  
        asks = pd.DataFrame.from_dict(order_book["asks"]).iloc[:, 0:2]
        asks.columns = ['ask_price', 'ask_qty']
        
        data = pd.concat([asks, bids])

        return data


    def get_historical_klines(self, symbol, interval, start_str, end_str):
        return self.client.get_historical_klines(symbol, interval, start_str, end_str)

    def get_transactions(self, symbol):
        if isinstance(symbol,str):
            return self.client.get_recent_trades(symbol=symbol)
        else:
            raise Exception("Symbol should be an instance of string.")
    
    def test_order(self, symbol, quantity):
        test_order = self.client.create_test_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=Client.ORDER_TYPE_MARKET,
            quantity=quantity)
        return test_order


def unix2date(timestamp):
    date = int(timestamp)/1000
    return datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')


DATA_DIR = '/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task2/'

bd = binance_data("fF1krtgLynipQf8yOZ1h1iGRGZM2K6ukAelCCsu05AE2gZl4fcQEVBosKXINw3IT", 
    "NsEoj50t36tTt8sFYcnihGJvmjf648agQHZIgknerDcHdDYmndFlAnihMBbflulj")


data_MD = bd.get_order_book(symbol="ETHBTC")
data_MD.to_csv(os.path.join(DATA_DIR, "market_depth.csv"), index=False)

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