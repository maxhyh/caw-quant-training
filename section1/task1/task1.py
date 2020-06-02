import datetime
import time
import re

import pandas as pd
import numpy as np
import requests

def getHTML(url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""
    data = r.json()
    return data

def get_data(fsym, tsym, freq, start_time=None, end_time=None, limit=None, e='CCCAGG'):
    fsym = fsym.upper()
    tsym = tsym.upper()
    agg = re.findall(r"\d+", freq)[0]
    freq = re.findall(r"[a-z]", freq)[0]

    if freq == 'h':
        freq_url = "/histohour?fsym={}&tsym={}".format(fsym, tsym)
    else:
        raise ValueError('frequency', freq, 'not supported')

    freq_url += f'&aggregate={agg}'
    freq_url += f'&e={e}'

    if start_time != None and end_time != None and limit == None:
        start_unix = int(pd.to_datetime(start_time).timestamp())
        end_unix = int(pd.to_datetime(end_time).timestamp())
        freq_url += f'&limit={2000}'  # limit
        query_url = freq_url + f'&toTs={end_unix}'  # until
        bottom_df = pd.DataFrame(getHTML(url + query_url))
        if len(bottom_df) == 0:
                raise Exception(f'No Data Fetched with {url + query_url}')
            while True:
                old_unix = bottom_df.iloc[0]['time']
                if old_unix <= start_unix:
                    bottom_df = bottom_df[bottom_df['time'] >= start_unix]
                    break
                else:
                    query_url = freq_url + f'&toTs={old_unix}'
                    query_df = pd.DataFrame(
                        getHTML(url + query_url))
                    if len(query_df) == 0:
                        request_time = datetime.datetime.utcfromtimestamp(
                            start_unix).strftime('%Y-%m-%d %H:%M:%S')
                        earlies_time = datetime.datetime.utcfromtimestamp(
                            old_unix).strftime('%Y-%m-%d %H:%M:%S')
                        print(
                            f"Request from {request_time}. But Available from {earlies_time}")
                        break
                    else:
                        bottom_df = query_df.append(bottom_df.iloc[1:], ignore_index=True)
            return bottom_df

def unix2date(unix, fmt="%Y-%m-%d %H:%M:%S"):
    """
        Convert unix epoch time 1562554800 to
        datetime with format
    """
    date = datetime.datetime.utcfromtimestamp(unix)
    return date.strftime(fmt)


def date2unxi(date, fmt="%Y-%m-%d %H:%M:%S"):
    """
        Convert datetime with format to 
        unix epoch time 1562554800
    """
    return int(time.mktime(time.strptime(date, fmt)))


def cc2bt(df):
    """Convert CryptoCompare data to Backtrader data
    """
    df['datetime'] = df['time'].apply(unix2date)
    df.drop(columns=['time'], inplace=True)
    df.rename(columns={'volumefrom': 'volume',
                       'volumeto': 'baseVolume'}, inplace=True)
    return df

def main():
    url = 'https://min-api.cryptocompare.com/data'
    DATA_DIR = '/Users/yihuihuang/Desktop'

    df = get_data('BTC', 'USDT', '1h', start_time="2017-04-01",
                        end_time="2020-08-22", e='binance')
    df = cc2bt(df)
    df.to_csv(os.path.join(DATA_DIR, "BTC_USDT_1h.csv"), index=False)

main()



