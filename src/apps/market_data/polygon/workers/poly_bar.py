'''
Created on Nov 20, 2021

@author: dan

Issue 

'''
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import argparse
import datetime
import requests
import json
import os
from multiprocessing import Pool

from src.apps import settings
from src.core import plumbing

from src.apps.data.core.workers import completed
from src.apps.data.core.models import bar_avail
from src.apps.data.polygon.models import poly_bar
from src.apps.data.polygon.models import day_raw


def get_date_time(ts):
    d = datetime.datetime.fromtimestamp(ts / 1000.0) + datetime.timedelta(hours=3)
    date = '%d%02d%02d' % (d.year, d.month, d.day)
    time = '%02d:%02d:%02d' % (d.hour, d.minute, d.second)
    return (date,time)
    
def parse(symbol, x):
    tx_keys = {'o':'open', 
               'h':'high',        
               'l':'low', 
               'c':'close', 
               'vw':'average', 
               'v':'volume', 
               'n':'count',
              }
    date,time = get_date_time(x['t'])
    r = {'symbol': symbol,
         'date':date,
         'time': time}
    for (old,new) in tx_keys.items():
        if old in x:
            r[new] = x[old]
    return r

def run_query(symbol, start, end):
    hyphen_start = plumbing.hyphen_date(start)
    hyphen_end = plumbing.hyphen_date(end)
    args = (symbol,
            hyphen_start,
            hyphen_end,
            'false', 
            settings.POLYGON_KEY)
    url = '''https://api.polygon.io/v2/aggs/ticker/%s/range/1/minute/%s/%s?adjusted=%s&sort=asc&limit=50000&apiKey=%s''' % args
    logging.debug(url)
    page = requests.get(url)
    j = json.loads(page.text)
    logging.info('| received | %8s | start=%8s | end=%8s | found: %d' % (symbol, start, end, j['resultsCount']))
    assert j['status'] == 'OK','query did not run properly: %s' % str(j)
    return j.get('results',[])
    
def get_poly_bars(symbol, days):
    n = 50 # chunk size
    raw = []
    while len(days) > 0:
        dd = days[:n]
        raw += run_query(symbol, dd[0], dd[-1])
        if len(dd) == n:
            days = days[n:]
        else:
            days = []
    rpt = []
    for x in raw:
        rpt += [parse(symbol, x)]
    return rpt

def run_one_day(args) -> int:
    symbol, date = args
    table_class = poly_bar.PolyBar()
    rpt = get_poly_bars(symbol, [date])
    if len(rpt)> 0:
        n = table_class.post(rpt)
        table_class.close_db()
        return n
    else:
        return 0

def get_universe(date):
    sql = '''select symbol from day_raw where date='%s' order by volume''' % (date)
    dr = day_raw.DayRaw()
    raw = dr.query(sql, ['symbol'])
    return list(map(lambda x: x['symbol'], raw))

def get_available(date):
    sql = '''select distinct(symbol) from poly_bar where date='%s';''' % date
    pa = poly_bar.PolyBar()
    raw = pa.query(sql, ['symbol'])
    return list(map(lambda x: x['symbol'], raw))

def get_where(start, end):
    if start is None:
        assert end is None
        where = ''
    elif end is None:
        where = "WHERE date>='%s'" % start
    else:
        where = "WHERE date>='%s' AND date<='%s'" % (start,end)
    return where

def get_good_poly_am_tables(start:str=None, end:str=None) -> list:
    '''
    input:
        start       : date 'yyyymmdd'
        end         : date 'yyyymmdd'
    output:
        list of dates where poly_am is good
    '''
    where = get_where(start, end)
    sql = '''SELECT date, 
                    SUM(CASE WHEN poly_am IS NOT NULL THEN 1 ELSE 0 END) ,
                    SUM(CASE WHEN poly_am IS NOT NULL THEN poly_am ELSE 0 END) 
            FROM bar_avail %s
            GROUP BY date
            ORDER BY date''' % where
    logging.debug(sql)
    ba = bar_avail.BarAvail()
    raw = ba.query(sql, ['date','symbols','rows'])
    switch = {True:1400000/2,
              False:1400000}
    goods = []
    for x in raw:
        threshold = switch[x['date'] in plumbing.HALF_DAYS]
        if x['rows'] > threshold:
            goods += [x['date']]
    logging.debug('GOODS: %s' % goods)
    return goods

def get_available_days():
    pb = poly_bar.PolyBar()
    sql = '''select distinct(date) from poly_bar order by date desc limit 20'''
    raw = pb.query(sql, ['date'])
    pb.close_db()
    
    pb_days = sorted(list(map(lambda x: x['date'], raw)))
    end = plumbing.get_most_recent_day(cutoff=57600)
    
    am_days = get_good_poly_am_tables(start=pb_days[0],
                                      end=end)
    return sorted(list(set(pb_days) | set(am_days)))

def rerun(date:str, n=8) -> int:
    required = get_universe(date)
    logging.info('%s | %d required symbols' % (date, len(required)))
    #end = plumbing.get_most_recent_day(cutoff=57600)
    #days = plumbing.get_trading_days('20210101', end)
    available = get_available(date)
    logging.info('%s | %d available symbols' % (date, len(available)))
    symbols = sorted(list(set(required) - set(available)))
    logging.info('%s | %d missing symbols' % (date, len(symbols)))
    to_do_list = []
    for symbol in symbols:
        #logging.info('backfill: %s on %s' % (symbol, date))
        to_do_list += [(symbol, date)]
        #print(to_do_list[-1])
        
    logging.info('running poly worker for %s symbols in a pool of n=%d' % (len(symbols),n))
    with Pool(n) as p:
        print(p.map(run_one_day, to_do_list))
    return len(to_do_list)

def create_table():
    assert False
    pb = poly_bar.PolyBar()
    pb.create_table()
    pb.close_db()

def main() -> None:
    end = plumbing.get_most_recent_day(cutoff=57600)
    required_days = plumbing.get_last_trading_days(end, 18) + [end]
    available_days = get_available_days()
    missing_days = sorted(list(set(required_days) - set(available_days)))
    print(missing_days)
    n = []
    for day in missing_days:
        logging.info('processing: %s' % day)
        n += rerun(day)
    
    full_file = os.path.abspath(__file__)
    completed.post(full_file=full_file, 
               request={'days': missing_days}, 
               result= {'n':n},
               done=n>0)

if __name__ == '__main__':
    logging.info('start')
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', '-d',help="trading date (none runs most recent)",type=str, default=None)
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    if args.date is None:
        main()
    else:
        rerun(args.date)