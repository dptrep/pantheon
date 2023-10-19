'''
Created on Nov 19, 2021

@author: dan
'''
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import requests
import json
import os

from src.apps import settings
from src.core import plumbing
from src.apps.data.core.workers import completed
from src.apps.data.polygon.models import day_raw

def parse(date, x):
    tx_keys = {'v':'volume', 
               'vw':'vwap', 
               'o':'open', 
               'c':'close', 
               'h':'high', 
               'l':'low', 
               'n':'n',
               'T':'symbol'}
    r = {'date': date}
    for (old,new) in tx_keys.items():
        if old in x:
            r[new] = x[old]
    return r

def fetch(day, split_adjusted=False):
    hyphen_date = plumbing.hyphen_date(day)
    url = '''https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/%s?adjusted=%s&apiKey=%s''' %(hyphen_date, str(split_adjusted).lower(), settings.POLYGON_KEY)
    page = requests.get(url)
    j = json.loads(page.text)
    logging.info('%s found: %d' % (day, j['resultsCount']))
    d = {}
    for x in j['results']:
        r = parse(day, x)
        d[r['symbol']] = r
    return list(d.values())

def get_available_days(lookback=20):
    dr = day_raw.DayRaw()
    sql = '''select distinct date from day_raw order by date desc limit %d''' % lookback
    raw = dr.query(sql, ['date'])
    dr.close_db()
    return [x['date'] for x in raw]

def populate(days):
    dr = day_raw.DayRaw()
    count = 0
    for n,day in enumerate(days):
        logging.info('%d of %d' % (n+1, len(days)))
        rpt = fetch(day)
        count += dr.post(rpt)
    dr.close_db()
    return count

def create_table():
    assert False,'you may be trying to initialize day_raw by mistake!'
    dr = day_raw.DayRaw()
    dr.create_table()
    dr.close_db()

def main():
    end = plumbing.get_most_recent_day(cutoff=57600)
    required_days = plumbing.get_last_trading_days(end, 18) + [end]
    available_days = get_available_days(lookback=len(required_days))
    missing_days = sorted(list(set(required_days) - set(available_days)))
    if len(missing_days) > 0:
        n = populate(missing_days)
        full_file = os.path.abspath(__file__)
        completed.luigi.post(full_file=full_file, 
                             request={'days': missing_days}, 
                             result= {'n':n},
                             done=n>0) 
    else:
        logging.info('no missing days')


if __name__ == "__main__":
    main()
    #populate(['20230303'])