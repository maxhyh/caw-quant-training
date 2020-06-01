import requests
import pandas as pd
from datetime import datetime


fsym = 'BTC'
tsym = 'USDT' 
start_time="2017-04-01"
end_time="2020-04-01"
e='binance'
datetime_interval = 'hour'

def get_filename(fsym, tsym, e, datetime_interval, end_time):
    return '%s_%s_%s_%s_%s.csv' % (fsym, tsym, e, datetime_interval, end_time)

def download_data(fsym, tsym, e, datetime_interval):
    supported_intervals = {'minute', 'hour', 'day'}
    assert datetime_interval in supported_intervals,\
        'datetime_interval should be one of %s' % supported_intervals
    print('Downloading %s trading data for %s %s from %s' %
          (datetime_interval, fsym, tsym, e))
    base_url = 'https://min-api.cryptocompare.com/data/histo'
    url = '%s%s' % (base_url, datetime_interval)
    params = {'fsym': fsym, 'tsym': tsym,
              'limit': 2000, 'aggregate': 1,
              'e': e}
    request = requests.get(url, params=params)
    data = request.json()
    return data

def convert_to_dataframe(data):
    df = pd.io.json.json_normalize(data, ['Data'])
    df['datetime'] = pd.to_datetime(df.time, unit='s')
    df = df[['datetime', 'low', 'high', 'open',
             'close', 'volumefrom', 'volumeto']]
    return df

def filter_empty_datapoints(df):
    indices = df[df.sum(axis=1) == 0].index
    print('Filtering %d empty datapoints' % indices.shape[0])
    df = df.drop(indices)
    return df

def main():
    data = download_data(fsym, tsym, e, datetime_interval)
    df = convert_to_dataframe(data)
    df = filter_empty_datapoints(df)
    current_datetime = datetime.now().date().isoformat()
    filename = get_filename(fsym, tsym, e, datetime_interval, current_datetime)
    print('Saving data to %s' % filename)
    df.to_csv(filename, index=False)
main()

