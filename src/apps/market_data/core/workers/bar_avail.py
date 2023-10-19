'''
intraday bar data availability

by: Dan Trepanier

Sep 10, 2022

ib_trades = 30 second bars
poly_bar  = 60 second bars

'''

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import numpy as np
import pandas as pd
import argparse
import os
#import datetime


from src.core import plumbing
from src.apps.data.core.models import bar_avail
from src.apps.data.ib.models import contracts
from src.apps.data.ib.models import ib_trades
from src.apps.data.ib.models import ib_vol

from src.apps.data.polygon.models import poly_bar
from src.apps.data.polygon.models import poly_am
from src.apps.data.core.workers import completed

def get_ib_trades(start, end=None):
    if end is None:
        end = start
    sql = '''SELECT date,symbol, con_id, count(*) 
             FROM ib_trades 
             WHERE date>='%s' AND date<='%s' 
             GROUP BY date,symbol,con_id;
            ''' % (start, end)
    logging.info(sql)
    ib = ib_trades.IbTrades()
    raw = ib.query(sql, ['date','symbol','con_id','ib_trades'])
    logging.info('%s | ib_trades: %d records' % (start, len(raw)))
    if len(raw) == 0:
        return None
    else:
        return pd.DataFrame(raw)


def get_ib_vol(start, end=None):
    if end is None:
        end = start
    sql = '''SELECT date, symbol, con_id,count(*) 
             FROM ib_vol  
             WHERE date>='%s' AND date<='%s' 
             GROUP BY date,symbol,con_id;
            ''' % (start, end)
    logging.info(sql)
    ib = ib_vol.IbVol()
    raw = ib.query(sql, ['date','symbol','con_id','ib_vol'])
    logging.info('%s | ib_vol: %d records' % (start, len(raw)))
    if len(raw) == 0:
        return None
    else:
        return pd.DataFrame(raw)

def get_poly_bar(start, end=None):
    if end is None:
        end = start
    
    sql = '''SELECT date,symbol, count(*) 
             FROM poly_bar 
             WHERE date>='%s' AND date<='%s' 
             GROUP BY date,symbol;
            ''' % (start, end)
    logging.info(sql)
    pb = poly_bar.PolyBar()
    raw = pb.query(sql, ['date','symbol','poly_bar'])
    logging.info('%s | poly_bar: %d records' % (start, len(raw)))
    if len(raw) == 0:
        return None
    else:
        return pd.DataFrame(raw)

def get_poly_am(date):
    sql = '''SELECT EXISTS (
       SELECT FROM information_schema.tables 
       WHERE  table_schema = 'public'
       AND    table_name   = 'poly_am_%s'
       );
    ''' % date
    logging.info(sql)
    pb = poly_bar.PolyBar()
    raw = pb.query(sql, ['exists'])
    if raw[0]['exists']:
        sql = '''SELECT date,symbol, count(*) 
                 FROM poly_am_%s 
                 GROUP BY date,symbol;
                ''' % (date)
        pa = poly_am.PolyAM(date)
        raw = pa.query(sql, ['date','symbol','poly_am'])
        logging.info('%s | poly_am: %d records' % (date, len(raw)))
        if len(raw) == 0:
            return None
        else:
            return pd.DataFrame(raw)
    else:
        return None

def get_contracts(symbols):
    ss = ','.join(list(map(lambda s: "'%s'" % s, symbols)))
    sql = '''SELECT posting_date, con_id, symbol 
             FROM contracts 
             WHERE symbol in (%s)
             AND sec_type='STK';''' % ss
    logging.info(sql)
    c = contracts.Contracts()
    raw = c.query(sql, ['date','con_id', 'symbol'])
    logging.info('found %d con_ids' % len(raw))
    return pd.DataFrame(raw)

def fetch(start, end=None):
    ib = get_ib_trades(start, end)
    iv = get_ib_vol(start, end)
    pb = get_poly_bar(start, end)
    c = None
    lst = [('ib_trades', ib),
           ('ib_vol', iv),
           ('poly_bar', pb)]
    missing = []
    for (label,n) in lst:
        if n is None:
            missing += [label]
        else:
            if c is None:
                c = n
            else:
                c = c.merge(n, left_on=['date','symbol'], 
                                right_on=['date','symbol'], 
                                how='outer')
    if end is None:
        days = [start]
    else:
        days = plumbing.get_trading_days(start, end)
    pa = pd.DataFrame()
    for day in days:
        n = get_poly_am(day)
        if n is not None:
            pa = pa.append(n)
        
    if len(pa) == 0:
        missing += ['poly_am']
    else:
        if c is None:
            c = pa
        else:
            c = c.merge(pa, left_on=['date','symbol'], 
                                    right_on=['date','symbol'], 
                                    how='outer')
    if c is None:
        return []
    else:
        symbols = c['symbol'].unique()
        logging.info('found %d symbols' % len(symbols))
        cc = get_contracts(symbols)
        if 'con_id' in c.columns:
            c = c.merge(cc, left_on=['symbol','con_id'],
                            right_on=['symbol','con_id'],
                            suffixes=['','_cont'],
                            how='outer'
                       )
        else:
            c = c.merge(cc, left_on=['symbol'],
                            right_on=['symbol'],
                            suffixes=['','_cont'],
                            how='outer'
                       )
        for column in missing:
            c[column] = np.nan
        c['posting_date'] = plumbing.today(format='date')
        c['posting_time'] = plumbing.current_time(time_unit='s',normal=True, time_zone='EST')

        ba = bar_avail.BarAvail()
        c = c[ba.columns[1:]] 
        tmp = c.fillna(0)
        tmp['total'] = tmp['ib_trades'] + tmp['ib_vol'] + tmp['poly_bar'] + tmp['poly_am']
        sub = tmp[tmp['total'] > 0]
        new = c.iloc[sub.index]
        for (c,t) in zip(ba.columns, ba.types):
            if t == 'BIGINT':
                new[c] = new[c].astype(int, errors='ignore')
        return new


def merge(old, new):
    c = old.merge(new, 
                  left_on=['date','symbol','con_id'],
                  right_on=['date','symbol','con_id'],
                  suffixes = ['_old','_new'],
                  how='outer')
    c.fillna(0, inplace=True)
    c['diff'] = 0
    c['prior'] = 0
    for label in ('ib_trades', 'poly_bar','poly_am'):
        c['%s_diff' % label] = c['%s_new' % label] - c['%s_old' % label]
        c['diff'] = c['diff'] + c['%s_diff' % label]
        c['prior'] = c['prior'] + c['%s_old' % label]
    
    ba = bar_avail.BarAvail()
    obsoletes = c[(c['prior'] > 0) & (c['diff'] > 0)]
    if len(obsoletes) > 0:
        print('OBSOLETES\n', obsoletes)
        lst = list(set(obsoletes['id'].values))
        uu = ','.join(list(map(lambda i: str(int(i)), lst)))
        sql = '''delete from bar_avail where id in (%s)''' % uu
        print(sql)
        ba.delete(sql, check=False)
        
    updates = c[c['diff'] > 0.1]
    if len(updates) > 0:
        #print("UPDATES\n", updates)
        new = updates
        cc = ['posting_date_new','posting_time_new',
              'date','symbol','con_id',
              'ib_trades_new',
              'ib_vol_new',
              'poly_bar_new',
              'poly_am_new']
        new = updates[cc]
        new.columns = ba.columns[1:]
        for (c,t) in zip(ba.columns, ba.types):
            if t == 'BIGINT':
                new[c] = new[c].astype(int, errors='ignore')
                new[c].replace(0, np.nan, inplace=True)
    return new

def populate(start, end):
    ba = bar_avail.BarAvail()
    sql = '''select * from bar_avail 
             WHERE date>='%s' AND date<='%s';
            ''' % (start, end)
    raw = ba.query(sql, ba.columns)
    old = pd.DataFrame(raw)
    new = fetch(start, end)
    logging.info('fetch %s to %s found: %d' % (start, end, len(new)))
    if len(old) > 0 and len(new) > 0:
        logging.info('merging old (%d) and new (%d) data' % (len(old), len(new)))
        new = merge(old, new)
    raw = new.to_dict('records')
    ba.post(raw)
    return len(raw)

def rerun(date):
    ba = bar_avail.BarAvail()
    sql = '''delete from bar_avail where date='%s';''' % date
    logging.info(sql)
    ba.delete(sql, check=False)
    logging.info('fetching data for: %s' % date)
    new = fetch(date, date)
    logging.info('fetch %s found: %d' % (date, len(new)))
    raw = new.to_dict('records')
    ba.post(raw)

def get_available_days(lookback=20):
    ba = bar_avail.BarAvail
    sql = '''select distinct date from bar_avail order by date desc limit %d''' % lookback
    raw = ba.query(sql, ['date'])
    ba.close_db()
    return [x['date'] for x in raw]

def create_table():
    assert False,'you may be trying to initialize day_raw by mistake!'
    ba = bar_avail.BarAvail()
    ba.create_table()
    ba.close_db()

def main():
    end = plumbing.get_most_recent_day(cutoff=57600)
    required_days = plumbing.get_last_trading_days(end, 18) + [end]
    available_days = get_available_days(lookback=len(required_days))
    missing_days = sorted(list(set(required_days) - set(available_days)))
    
    s = min(missing_days)
    e = max(missing_days)
    n = populate(s,e)
    
    full_file = os.path.abspath(__file__)
    completed.post(full_file=full_file, 
               request={'days': missing_days}, 
               result= {'n':n},
               done=n>0) 
 
    return n

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', '-d',help="start trading date (none runs most recent)",type=str, default=None)
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    if args.date is None:
        main()
    else:
        rerun(args.date)
    