from binance.client import Client
from datetime import datetime
import os
import pandas as pd
from binance.websockets import BinanceSocketManager

class binance_data():

    def __init__(self):
        self.client = Client("","")

    def get_order_book(self, symbol):

        depth = self.client.get_order_book(symbol = symbol)

        ID = depth['lastUpdateId']

        df_depth_asks = depth['asks']
        ask_price = df_depth_asks[0]
        ask__qty = df_depth_asks[1]

        df_depth_bids = depth['bids']
        bid_price = df_depth_bids[0]
        bid_qty = df_depth_bids[1]

        order_book = pd.DataFrame({'ID':ID, 'Ask Price': ask_price, 'Ask Quantity': ask__qty,
                    'Bid Price': bid_price, 'Bid Quantity': bid_qty})
        return order_book


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

data_MD = bd.get_order_book("ETHBTC")
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
