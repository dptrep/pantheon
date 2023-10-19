'''
populates poly opt master table

by: Dan Trepanier
date: Jan 2, 2023



'''


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import requests
import json
import os

from src.apps import settings
from src.core import plumbing
from src.apps.data.core.workers import completed
from src.apps.data.polygon.models import poly_opt_master


def parse(x):
    x['posting_date'] = plumbing.today()
    x['expiration_date'] = x['expiration_date'].replace('-','')
    return x

def fetch(j, max_attempts=3) -> dict:
    api_key = '&apiKey=' + settings.POLYGON_KEY
    url = j['next_url'] + api_key
    n = 0
    while n < max_attempts:
        n += 1
        try:
            page = requests.get(url)
            j = json.loads(page.text)
            return j
        except:
            logging.warning('problem fetching: %s' % url)
    logging.warning('unable to run last query: %s' % url)
    

def run_query(date):
    hyphen_date = plumbing.hyphen_date(date)
    request_url = 'https://api.polygon.io/v3/reference/options/contracts?as_of=%s&limit=1000&apiKey=hXWzmnDY7oPvNMdIyGpmGapeLBd8c9oY' % hyphen_date
    j = {'next_url': request_url}
    n = 0
    count = 0
    pom = poly_opt_master.PolyOptMaster()
    while 'next_url' in j:
        n += 1
        logging.info('%s | polygon | page=%d | count=%d' % (date, n, count))
        j = fetch(j)
        if 'results' in j:
            rpt = list(map(lambda x: parse(x), j['results']))
            count += len(rpt)
            pom.post(rpt, on_conflict='DO NOTHING')
        else:
            logging.info('unexpected end: %s' % j)
            break
    return count

def catch_up(start, end):
    '''
    input:
        start : 'yyyymmmdd'

    output:
        count
    '''
    days = plumbing.get_trading_days(start, end)
    last_mm = '201912'
    count = 0
    for day in days:
        print(day)
        n = plumbing.get_day_of_week(day)
        mm = day[:6]
        if n == 4 and mm != last_mm:
            logging.info('%s | fetching data' % day)
            n = run_query(day)
            count += n
            logging.info('%s | posted=%10d | cumulative=%10d |' %(day, n, count))
            last_mm = mm
    return count

def create_table():
    assert False,'you may be trying to initialize day_raw by mistake!'
    pom = poly_opt_master.PolyOptMaster()
    pom.create_table()
    pom.close_db()

def main():
    day = plumbing.today()
    pom = poly_opt_master.PolyOptMaster()
    max_date = pom.get_max_date('posting_date')
    weekday = plumbing.get_day_of_week(day)
    if day > max_date and weekday in (0,4):
        logging.info('%s | fetching data' % day)
        n = run_query(day)
        logging.info('%s | posted: %d' %(day, n))
        new_max_date = pom.get_max_date('posting_date')
        
        full_file = os.path.abspath(__file__)
        completed.post(full_file=full_file, 
                        request={'date':new_max_date}, 
                        result= {'n':n},
                        done=n>0) 

if __name__ == '__main__':
    main()