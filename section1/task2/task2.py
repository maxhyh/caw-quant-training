import time
import dateparser
import pytz
import json
import os
import pandas as pd

from datetime import datetime
from binance.client import Client


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms


def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    """
    # create the Binance client, no need for api key
    client = Client("", "")

    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    return output_data

def get_transactions(symbol):
    """Get transactions from Binance
    """

    client = Client("", "")
    transactions_data = []
    limit = 500
    timeframe = interval_to_milliseconds(interval)
    start_ts = date_to_milliseconds(start_str)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0

    symbol_existed = False
    while True:
        trade_data = client.get_recent_trades(symbol=symbol)

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            output_data += temp_data
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            start_ts += timeframe

        idx += 1
        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

    return transactions_data

def get_order_book(symbol):
    """Get Market Depth from Binance
    """

    client = Client("", "")
    market_data = []
    limit = 500
    timeframe = interval_to_milliseconds(interval)
    start_ts = date_to_milliseconds(start_str)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0

    symbol_existed = False
    while True:
        order_data = client.get_order_book(symbol=symbol)

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            output_data += temp_data
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            start_ts += timeframe

        idx += 1
        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

    return market_data


symbol = "ETHBTC"
start = "1 Apr, 2017"
end = "1 Apr, 2020"
interval = Client.KLINE_INTERVAL_30MINUTE

klines = get_historical_klines(symbol, interval, start, end)
trade = get_transactions(symbol)
order_book = get_order_book(symbol)
