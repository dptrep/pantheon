'''
split adjustor

by: Dan Trepanier

July 23, 2022
'''
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import argparse
import copy
import pandas as pd
from tabulate import tabulate
from src.core import plumbing

from src.apps.market_data.models import dividends
from src.apps.market_data.models import splits
from src.apps.market_data.models import day_raw

def get_interesting_symbols(symbols:list, days:list) -> list:
    ss = ','.join(list(map(lambda s: "'%s'" % s, symbols)))
    start = plumbing.get_last_trading_day(min(days))
    sql = '''select distinct(symbol) from splits where symbol in (%s) and date>='%s' and date<='%s';''' % (ss, start, max(days))
    s = splits.Splits()
    raw = s.query(sql, ['symbol'])

    sql = '''select distinct(symbol) from dividends where symbol in (%s) and ex_dividend_date>='%s' and ex_dividend_date<='%s';''' % (ss, start, max(days))
    d = dividends.Dividends()
    raw += d.query(sql, ['symbol'])
    lst = map(lambda x: x['symbol'], raw)
    return sorted(list(set(lst)))

def get_dividends(symbol:str, days:list) -> dict:
    start = plumbing.get_last_trading_day(min(days))
    sql = '''select * from dividends where symbol='%s' and ex_dividend_date>='%s' and ex_dividend_date<='%s';''' % (symbol, start, max(days))
    d = dividends.Dividends()
    raw = d.query(sql, d.columns)
    div_data = {}
    for x in raw:
        div_data[x['ex_dividend_date']] = x['cash_amount']
    return div_data

def get_splits(symbol:list, days:list) -> dict:
    start = plumbing.get_last_trading_day(min(days))
    sql = '''select * from splits where symbol='%s' and date>='%s' and date<='%s';''' % (symbol, start, max(days))
    s = splits.Splits()
    raw = s.query(sql, s.columns)
    split_data = {}
    for x in raw:
        split_data[x['date']] = x
    return split_data

def get_raw_prices(symbols:list, days:list) -> dict:
    ss = ','.join(list(map(lambda s: "'%s'" % s, symbols)))
    start = plumbing.get_last_trading_day(min(days))
    sql = '''select * from day_raw where symbol in (%s) and date>='%s' and date<='%s' order by symbol, date;''' % (ss, start, max(days))
    dr = day_raw.DayRaw()
    raw = dr.query(sql, dr.columns)
    return raw

def adjust(b, ratio):
    kk = ['open','high','low','close','vwap']
    new = copy.deepcopy(b)
    for k in kk:
        new[k] = plumbing.safe_multiply(b[k] , ratio, default=None)
    kk = ['volume']
    for k in kk:
        new[k] = plumbing.safe_divide(b[k] , ratio, default=None)
    new['ratio'] = ratio
    return new

def adjust_am(b, ratio):
    kk = ['open','high','low','close','vwap','day_vwap','average']
    new = copy.deepcopy(b)
    for k in kk:
        if k in b:
            new[k] = plumbing.safe_multiply(b[k] , ratio, default=None)
    kk = ['volume', 'avg_size', 'day_volume']
    for k in kk:
        if k in b:
            new[k] = plumbing.safe_divide(b[k] , ratio, default=None)
    new['ratio'] = ratio
    return new

def run_symbol(symbol, days ,bb, fmt='day_raw'):
    bb = sorted(bb, key=lambda k: k['date'], reverse=True)
    split_data = get_splits(symbol, days)
    div_data = get_dividends(symbol, days)
    ratio = 1
    lst = []
    for b in bb:
        b = adjust(b, ratio)
        if fmt == 'day_raw':
            b['ratio'] = ratio
            lst += [b]
        else:
            r = {'symbol': symbol,
                 'date': b['date'],
                 'ratio': ratio}
            lst += [r]
        if b['date'] in split_data:
            x = split_data[b['date']]
            if x['after'] > 0:
                ratio *= x['before'] / x['after']
        if b['date'] in div_data:
            amount = div_data[b['date']]
            ratio *= (1 - amount / b['close'])
    lst.reverse()
    return lst

def get_todays_ratios(symbols:list, date:str=None) -> dict:
    '''
    this the ratio to adjust today's prices to compare against yesterday's adjusted prices
    '''
    if date is None:
        date = plumbing.today()
    yesterday = plumbing.get_last_trading_day(date)
    days = [yesterday, date]
    interesting_symbols = get_interesting_symbols(symbols, days)
    ratios = {} # [symbol] = ratio
    if len(interesting_symbols) > 0:
        bars = get_raw_prices(interesting_symbols, days)    
        for symbol in symbols:
            if symbol in interesting_symbols:
                bb = list(filter(lambda x: x['symbol'] ==symbol, bars))
                lst = run_symbol(symbol, days, bb, fmt='ratio')
                ratio = 1.0 / lst[0]['ratio']
            else:
                ratio = 1.0
            ratios[symbol] = ratio
    return ratios

def adjustor(symbols, start, end, fmt='day_raw'):
    assert fmt in ('ratio', 'day_raw'),'requested: %s' % fmt
    nxt_day = plumbing.get_next_trading_day(end)
    days = plumbing.get_trading_days(start, nxt_day)
    interesting_symbols = get_interesting_symbols(symbols, days)
    bars = get_raw_prices(symbols, days)
    rpt = []
    for n,symbol in enumerate(symbols):
        logging.info('processing: %s | adjusting: %6s | %d of %d' % (symbol, symbol in interesting_symbols, n+1, len(symbols)))
        bb = list(filter(lambda x: x['symbol'] ==symbol, bars))
        if symbol in interesting_symbols:
            r = run_symbol(symbol, days, bb, fmt)
            if len(r) > 0 and r[-1]['date'] == nxt_day:
                r = r[:-1]
            rpt += r
        elif fmt == 'day_raw':
            rpt += bb
    return rpt

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start','--date','-s', '-d',help="start date",type=str, required=True)
    parser.add_argument('--end', '-e',help="end date",type=str, required=True)
    parser.add_argument('--symbol', help="symbol",type=str, default='AAPL')
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    rpt = adjustor([args.symbol],args.start, args.end)
    df = pd.DataFrame(rpt)
    print(tabulate(df, headers='keys', tablefmt='psql'))
    logging.info('end')