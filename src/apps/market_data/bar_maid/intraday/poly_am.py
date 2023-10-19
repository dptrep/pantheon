'''
process poly am bar

by: Dan Trepanier

Sept 21, 2022
'''

import datetime
import time

import prettytable
from src.core import plumbing

from src.apps.platform.models import poly_am
from src.apps.bar_maid.intraday import bar

def safe_date(d):
    if type(d) == str:
        if '-' in d:
            d = d.replace('-','')
        return datetime.date(int(d[:4]), int(d[4:6]), int(d[6:]))
    else:
        return d

def safe_time(t):
    if type(t) == str:
        return datetime.time(int(t[:2]), int(t[3:5]), int(t[6:]))
    else:
        return t

class Bar(bar.Bar):
    def __init__(self, symbol, bar_size, prior_bar):
        bar.Bar.__init__(self, symbol, bar_size, prior_bar)

    def post(self, b:dict) -> dict:
        self.data_source = 'poly_am'
        if b['start_time'] < datetime.time(9,30):
            self.session = 'pre'
        elif str(b['start_time']) >= plumbing.closing_time(b['date'], normal=True):
            self.session = 'after'
        else:
            self.session = 'day'

        # time metrics
        d = str(b['date'])
        if '-' in d:
            d = d.replace('-','')
            self.date = str(d)
            
        else:
            self.date = str(d)
        self.date_dt = safe_date(b['date'])
        t = str(b['end_time'])
        self.datetime = datetime.datetime(int(d[:4]), int(d[4:6]), int(d[6:]),
                                           int(t[:2]), int(t[3:5]), int(t[6:]))
        self.time = safe_time(b['end_time'])
        self.t = plumbing.abs_time(b['end_time'], time_unit='s')
        self.am_count = int((self.t - 60 - 34200) / 60)
        self.bar_count = int((self.t - 60 - 34200) / self.bar_size)
        if self.t <= 34200:
            self.am_count -= 1
            self.bar_count -= 1
            
        self.start_t = self.bar_count * self.bar_size + 34200
        self.end_t = self.start_t + self.bar_size

        self.start_time = plumbing.normal_time(self.start_t, time_unit='s', fmt='time')
        self.end_time = plumbing.normal_time(self.end_t, time_unit='s', fmt='time')

        self.pro_rata = (self.t - self.start_t) / self.bar_size
        
        self.new = self.pro_rata <= (60 / self.bar_size + .001) # start of new bar
        self.complete = self.pro_rata > .999999 # end of bar
        notional = plumbing.safe_multiply(b['vwap'], b['volume'], default=0)
        if self.bar_size == 60 or self.new or self.prior_bar is None:
            kk = ['sequence_number', 
                  'day_open','day_vwap','day_volume',
                  'open', 'high', 'low', 'close',
                  'volume', 'vwap', 'avg_size',
                  'msg_count']
            for k in kk:
                setattr(self, k, b[k])
            self.notional = notional
            self.travel = (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
            
        else:
            kk = ['sequence_number', 
                  'close',
                  'day_open','day_vwap','day_volume']
            
            for k in kk:
                setattr(self, k, b[k])
            self.open = self.prior_bar.open
            self.high = plumbing.safe_max([self.prior_bar.high, b['high']])
            self.low = plumbing.safe_min([self.prior_bar.low, b['low']])
            
            if not self.duplicate:
                kk = ['volume','msg_count']
                for k in kk:
                    last = getattr(self.prior_bar, k)
                    setattr(self, k, last + b[k])
                
                self.avg_size = plumbing.safe_divide(self.volume, self.msg_count, default=None)
                self.notional = plumbing.safe_sum([self.prior_bar.notional, notional], default=0)
                self.vwap = plumbing.safe_divide(self.notional, self.volume, default=None)
                self.travel = self.prior_bar.travel + (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
        
        self.day_low = plumbing.safe_min([self.day_low, b['low']])
        self.day_high = plumbing.safe_max([self.day_high, b['high']])
        
        if self.prior_bar is None or self.prior_bar.date != self.date:
            self.day_travel = self.travel
            self.day_notional = notional
            self.day_msg_count = b['msg_count']
        else:
            self.day_travel = self.prior_bar.day_travel + (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
            self.day_notional = self.prior_bar.day_notional + notional
            self.day_msg_count = self.prior_bar.day_msg_count + b['msg_count']
            if self.bar_count > self.prior_bar.bar_count:
                self.prior_bar.close_out()
            
            self.duplicate = self.prior_bar.datetime == self.datetime
            self.skipped_am = self.am_count - self.prior_bar.am_count - 1
            self.skipped_full = self.bar_count - self.prior_bar.bar_count - 1

            if self.prior_bar.complete:
                self.last_close = self.prior_bar.close
            else:
                self.last_close = self.prior_bar.last_close

            if self.last_close is not None:
                self.ret = plumbing.safe_divide(10000 * (self.close - self.last_close), self.last_close, default=None)
            assert self.datetime >= self.prior_bar.datetime,'out of sequence bars\nlast: %s\nnew: %s' % (self.prior_bar.get(), self.get())
        return self.get()

def get_single_stream(symbol, date):
    sql = '''SELECT * 
             FROM poly_am_%s 
             WHERE symbol='%s' 
             ORDER BY end_time;''' % (date, symbol)
    pa = poly_am.PolyAM(date)
    return pa.query(sql)

def run_single(symbol, date, bar_size):
    #s = adjustor.get_adjustment_ratios([symbol], date, adjustment_date)
    raw = get_single_stream(symbol, date)
    bb = []
    bar =  None
    for b in raw:
        #k = (b['symbol'], b['date'])
        #ratio = s.get(k, 1)
        bar = Bar(symbol, bar_size, prior_bar=bar)
        bar.post(b)
        bb += [bar]
    return bb

def run_multi(kk:list, bar_size:int=60) -> dict:
    '''
    where kk = [(symbol, date), ...]
    '''
    symbols = list(set(map(lambda k: k[0], kk)))
    days = sorted(list(set(map(lambda k: k[1], kk))))
    #s = adjustor.get_adjustment_ratios(symbols, min(days), adjustment_date)

    start_time = time.time()

    results = {} #[(symbol, date)] = [] list of bars

    for day in days:
        sub = filter(lambda k: k[1] == day, kk)
        symbols = list(set(map(lambda k: k[0], sub)))
        
        ss = ','.join(map(lambda s: "'%s'" % s, symbols))

        sql = '''SELECT * 
                FROM poly_am_%s  
                WHERE symbol in (%s)
                ORDER BY symbol,end_time;''' % (day, ss)
        #print(sql)
        pa = poly_am.PolyAM(day)
        raw = pa.query(sql)

        bar =  None
        bb = []
        for b in raw:
            if bar is not None and b['symbol'] != bar.symbol:
                k = (bar.symbol, bar.date)
                results[k] = bb
                bar = None
                bb = []
            k = (b['symbol'], b['date'])
            #ratio = s.get(k, 1)
            bar = Bar(b['symbol'], bar_size, prior_bar=bar)
            bar.post(b)
            bb += [bar]
    
        if bar is not None:
            k = (bar.symbol, bar.date)
            results[k] = bb
    end_time = time.time()
    #print('multi', end_time - start_time)
    return results
    
    
            
if __name__ == '__main__':
    symbol = 'AAPL'
    start = '20220915'
    end = '20220921'
    run_single(symbol, start, 60)


    
    symbols = ['AA','AAPL']
    days = plumbing.get_trading_days(start, end)
    kk = []
    for symbol in symbols:
        for day in days:
            kk += [(symbol, day)]
    
    results = run_multi(kk, 60)
    print('requests')
    print(kk)
    print('resutls')
    print(results.keys())
    for k in results.keys():
        print(k ,len(results[k]))
