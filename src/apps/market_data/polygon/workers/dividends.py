'''
dividend worker

by: Dan Trepanier

July 23, 2022
'''

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import requests
import json
import sys
import os

from src.apps import settings
from src.core import plumbing
from src.apps.data.core.workers import completed
from src.apps.data.polygon.models import dividends

def parse(x):
    today = plumbing.today()
    same = ['declaration_date','record_date',
            'ex_dividend_date','pay_date',
            'cash_amount','dividend_type',
            'frequency']
    r = {'posting_date':today, 'symbol': x['ticker']}
    for k in same:
        v = x.get(k, None)
        if 'date' in k and v is not None:
            v = v.replace('-','')
        r[k] = v
    return r

def fetch():
    start_date = plumbing.get_most_recent_day(cutoff=57600)
    api_key = '&apiKey=' + settings.POLYGON_KEY
    hyphen_date = plumbing.hyphen_date(start_date)
    j = {'next_url':'https://api.polygon.io/v3/reference/dividends?ex_dividend_date.gte=%s&order=asc' % hyphen_date}
    rpt = []
    n = 0
    while 'next_url' in j:
        n += 1
        logging.info('polygon page: %d' % n)
        url = j['next_url'] + api_key
        page = requests.get(url)
        j = json.loads(page.text)
        if 'results' in j:
            rpt += list(map(lambda x: parse(x), j['results']))
        else:
            logging.warning('unexpected end: %s' % j)
            break
    return rpt

def remove_duplicates():
    sql = '''DELETE FROM dividends a 
                    USING dividends b 
             WHERE a.id < b.id 
                    AND a.symbol=b.symbol 
                    AND a.declaration_date=b.declaration_date;'''
    table_class = dividends.Dividends()
    table_class.delete(sql_stmt=sql, check=False)

def post_clean(rpt:list) -> int:
    end = plumbing.get_most_recent_day(cutoff=57600)
    table_class = dividends.Dividends()
    sql = '''select symbol, ex_dividend_date from dividends where ex_dividend_date>='%s';''' % end
    raw = table_class.query(sql, ['symbol','date'])
    kk = list(map(lambda x: (x['symbol'], x['date']), raw))
    new = []
    for x in rpt:
        k = (x['symbol'], x['ex_dividend_date'])
        if k not in kk:
            new += [x]
    
    if len(new) > 0:
        n = table_class.post(new)
    else:
        n = 0
    return n

def create_table():
    assert False,'you may be trying to initialize day_raw by mistake!'
    d = dividends.Dividends()
    d.create_table()
    d.close_db()

def main(initialize=False):
    assert not initialize, 'do not want to initialize'
    rpt = fetch()
    n = post_clean(rpt)
    remove_duplicates()
    days = sorted(list(set(map(lambda x: x['ex_dividend_date'], rpt))))
    full_file = os.path.abspath(__file__)
    completed.post(full_file=full_file, 
                    request={'days': days}, 
                    result= {'n':n},
                    done=n>0) 

if __name__ == '__main__':
    main()