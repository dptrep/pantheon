'''
collect split data from polygon

by: Dan Trepanier

July 23, 2022
'''

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')

import requests
import json
import os
from multiprocessing import Pool

from src.core import plumbing
from src.apps.data.polygon.models import splits
from src.apps.data.core.workers import completed


def parse(x):
    tx = {'execution_date': 'date',
          'split_from': 'before',
          'split_to': 'after',
          'ticker':'symbol'}
    n = {}
    for (old, new) in tx.items():
        n[new] = x[old]
    return n

def fetch():
    start_date = plumbing.get_most_recent_day(cutoff=57600)
    hyphen_date = plumbing.hyphen_date(start_date)
    url = 'https://api.polygon.io/v3/reference/splits?execution_date.gte=%s&apiKey=hXWzmnDY7oPvNMdIyGpmGapeLBd8c9oY' % hyphen_date
    logging.info('poly query: %s' % url)
    page = requests.get(url)
    j = json.loads(page.text)
    rpt = []
    for x in j.get('results',[]):
        r = parse(x)
        rpt += [r]
    return rpt

def remove_duplicates():
    sql = '''DELETE FROM splits a 
                    USING splits b 
             WHERE a.id > b.id 
                    AND a.symbol=b.symbol 
                    AND a.date=b.date;'''
    table_class = splits.Splits()
    table_class.delete(sql_stmt=sql, check=False)

def post_clean(rpt:list) -> int:
    end = plumbing.get_most_recent_day(cutoff=57600)
    table_class = splits.Splits()
    sql = '''select date,symbol from splits where date>='%s';''' % end
    raw = table_class.query(sql, ['symbol','date'])
    kk = list(map(lambda x: (x['symbol'], x['date']), raw))
    
    new = []
    for x in rpt:
        k = (x['symbol'], x['date'])
        if k not in kk:
            kk += [k]
            new += [x]
    
    if len(new) > 0:
        n = table_class.post(new)
    else:
        n = 0
    return n

def create_table():
    assert False,'you may be trying to initialize day_raw by mistake!'
    s = splits.Splits()
    s.create_table()
    s.close_db()

def main():
    rpt = fetch()
    n = post_clean(rpt)
    remove_duplicates()
    days = sorted(list(set(map(lambda x: x['date'], rpt))))
    full_file = os.path.abspath(__file__)
    completed.post(full_file=full_file, 
                    request={'days': days}, 
                    result= {'n':n},
                    done=n>0) 
    
if __name__ == '__main__':
    main()