'''
generate day bars from day raw

by: Dan Trepanier

Sept 23, 2022
'''
import logging
import datetime
import prettytable

from src.core import plumbing
from src.apps.market_data.views import adjustor
from src.apps.market_data.models import day_raw
from src.apps.bar_maid.daily import bar

from src.apps.platform.models import poly_am

class Bar(bar.Bar):
    def __init__(self, symbol, prior_bar):
        bar.Bar.__init__(self, symbol, prior_bar)

    def post(self, b:dict) -> dict:
        self.data_source = 'day_raw'

        d = str(b['date'])
        if '-' in d:
            d = d.replace('-','')
            self.date_dt = b['date']
        else:
            self.date_dt = plumbing.get_date_format(d)

        self.date = d
        self.datetime = datetime.datetime(int(d[:4]), int(d[4:6]), int(d[6:]), 16,0,0)
        
        self.end_t = plumbing.closing_time(d, time_unit='s', normal=False)
        self.t = self.end_t
        
        self.end_time = plumbing.get_time_format(plumbing.closing_time(d, time_unit='s', normal=True))
        self.time = self.end_time

        # bar price metrics
        self.open = b['open']
        self.high = b['high']
        self.low = b['low']
        self.close = b['close']
        self.vwap = b['vwap']
        self.volume = b['volume']
        self.notional = plumbing.safe_multiply(b['volume'], b['vwap'],default=0)
        self.msg_count = b['n']
        self.avg_size = self.volume / self.msg_count

        self.complete = True
        self.pro_rata = 1.0

        if self.prior_bar is not None:
            recent = plumbing.get_last_trading_days(d, 10)
            recent.reverse()
            if self.prior_bar.date in recent:
                self.skipped_full = recent.index(self.prior_bar.date)
            else:
                self.skipped_full = None
            self.last_close = self.prior_bar.close
            
            self.duplicate = self.prior_bar.date == self.date

        self.ratio = b['ratio']
        self.dividend = b.get('dividend', 0)
        self.before = b.get('before',1)
        self.after = b.get('after',1)

        if None not in (self.close, self.last_close):
            last_ratio = self.prior_bar.ratio
            last_close = self.prior_bar.close * last_ratio
            new_close = self.close * self.ratio
            self.ret = plumbing.safe_divide(10000 * (new_close - last_close), last_close, default=None)

def run_symbol(symbol, days ,bb):
    #bb = sorted(bb, key=lambda k: k['date'], reverse=True)
    split_data = adjustor.get_splits(symbol, days)
    div_data = adjustor.get_dividends(symbol, days)
    ratio = 1
    lst = []
    for b in bb:
        #print(b)
        b['ratio'] = ratio
        lst += [b]
        if b['date'] in split_data:
            x = split_data[b['date']]
            b['before'] = x['before']
            b['after'] = x['after']
            if x['after'] > 0:
                ratio *= x['before'] / x['after']
        if b['date'] in div_data:
            amount = div_data[b['date']]
            b['dividend'] = amount
            ratio *= (1 - amount / b['close'])
    lst.reverse()
    new = []
    bar = None
    for b in lst:
        bar = Bar(symbol, bar)
        bar.post(b)
        new += [bar]
    return new

def run_multi_today(symbols):
    date = plumbing.today()
    ss = ','.join(list(map(lambda s: "'%s'" % s, symbols)))
    sql = '''SELECT DISTINCT ON (pa.symbol) 
                    pa.symbol, 
                    pa.date,pa.end_time, 
                    pa.day_open,
                    a.day_high, a.day_low, 
                    pa.close as close, 
                    pa.day_volume,
                    pa.vwap,
                    a.day_msg_count 
             FROM poly_am_%s pa 
             INNER JOIN 
                   (SELECT symbol, 
                            MAX(high) AS day_high, 
                            MIN(low) AS day_low,
                            SUM(msg_count) AS day_msg_count 
                    FROM poly_am_%s 
                    GROUP BY symbol) a 
             WHERE symbol in (%s) 
             ON pa.symbol=a.symbol  
             ORDER BY symbol, end_time DESC;''' % (date, date, ss)
    pa = poly_am.PolyAM(date)
    cc = ['symbol',
            'date','time',
         'open','high','low','close',
         'volume','vwap','n']
    raw = pa.query(sql, cc)
    return raw

def run_single(symbol, days):
    sql = '''select * 
             from day_raw 
             where symbol='%s' 
             and date>='%s' 
             and date<='%s' 
             order by date desc;''' % (symbol, min(days), max(days))
    dr = day_raw.DayRaw()
    bb = dr.query(sql, dr.columns)
    dd = list(map(lambda x: x['date'], bb))
    today = plumbing.today()
    if today in days and today not in dd:
        bb += run_multi_today([symbol])
    return run_symbol(symbol, days, bb)

def run_multi(symbols, days):
    results = {}
    for symbol in symbols:
        bb = run_single(symbol, days)
        for b in bb:
            k = (b.symbol, b.date)
            results[k] = b
    return results


if __name__ == '__main__':
    start = '20221001'
    today = plumbing.today()
    days = plumbing.get_trading_days(start, today)
    bb = run_single('AAPL',days)

    kk = ['date','time', 'last_close','open','high','low','close','ret','volume','before','after','ratio']
    pt = prettytable.PrettyTable(kk)
    for bar in bb:
        d = bar.get()
        row = list(map(lambda k: d[k], kk))
        pt.add_row(row)    
    print(pt)