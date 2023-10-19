'''
Poly bar parser

by: Dan Trepanier

Sep 21, 2022
'''
import datetime
import time
import prettytable
from src.core import plumbing

from src.apps.market_data.models import poly_bar
from src.apps.bar_maid.intraday import bar

class Bar(bar.Bar):
    def __init__(self, symbol, bar_size, prior_bar):
        bar.Bar.__init__(self, symbol, bar_size, prior_bar)

    def post(self, b:dict) -> dict:
        self.data_source = 'poly_bar'
        if b['time'] < datetime.time(9,30):
            self.session = 'pre'
        elif str(b['time']) >= plumbing.closing_time(b['date'], normal=True):
            self.session = 'after'
        else:
            self.session = 'day'

        # time metrics
        d = str(b['date'])
        if '-' in d:
            d = d.replace('-','')
            self.date = d
            self.date_dt = b['date']
        else:
            self.date = d
            self.date_dt = plumbing.get_date_format(d)
        self.t = plumbing.abs_time(b['time'], time_unit='s') + 60
        self.time = plumbing.normal_time(self.t, time_unit='s', fmt='time')

        t = str(self.time)
        self.datetime = datetime.datetime(int(d[:4]), int(d[4:6]), int(d[6:]),
                                           int(t[:2]), int(t[3:5]), int(t[6:]))
        
        self.am_count = int((self.t - 60 - 34200) / 60)
        self.bar_count = int((self.t - 60 - 34200) / self.bar_size)
        if self.t <= 34200:
            self.am_count -= 1
            self.bar_count -= 1
            
        #self.start_t = self.bar_count * self.bar_size + 34200
        self.start_t = plumbing.abs_time(b['time'], time_unit='s')
        self.end_t = self.start_t + self.bar_size

        self.start_time = plumbing.normal_time(self.start_t, time_unit='s', fmt='time')
        self.end_time = plumbing.normal_time(self.end_t, time_unit='s', fmt='time')

        self.pro_rata = (self.t - self.start_t) / self.bar_size
        
        self.new = self.pro_rata <= (60 / self.bar_size + .001) # start of new bar
        self.complete = self.pro_rata > .999999 # end of bar

        self.sequence_number = b['id']
        notional = plumbing.safe_multiply(b['average'], b['volume'], default=0)

        if self.bar_size == 60 or self.new or self.prior_bar is None:
            kk = ['open', 'high', 'low', 'close',
                  'volume', 
                  ]
            for k in kk:
                setattr(self, k, b[k])
            self.vwap = b['average']
            self.msg_count = b['count']
            self.avg_size = plumbing.safe_divide(b['volume'] , self.msg_count, None)
            self.notional = notional
            self.travel = (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
            
        else:
            kk = ['close',
                  #'day_open','day_vwap','day_volume'
                  ]
            
            for k in kk:
                setattr(self, k, b[k])
            self.open = self.prior_bar.open
            self.high = plumbing.safe_max([self.prior_bar.high, b['high']])
            self.low = plumbing.safe_min([self.prior_bar.low, b['low']])
            
            if not self.duplicate:
                #kk = ['volume','msg_count']
                #for k in kk:
                #    last = getattr(self.prior_bar, k)
                #    setattr(self, k, last + b[k])
                self.volume = self.prior_bar.volume + b['volume']
                self.msg_count = self.prior_bar.msg_count + b['count']
                self.avg_size = plumbing.safe_divide(self.volume, self.msg_count, default=None)
                self.notional = plumbing.safe_sum([self.prior_bar.notional, b['average'] * b['volume']], default=0)
                self.vwap = plumbing.safe_divide(self.notional, self.volume, default=None)
                self.travel = self.prior_bar.travel + (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
                self.day_travel = self.prior_bar.day_travel + (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']

        if self.session == 'day' and self.day_open is None:
            self.day_open = b['open']

        if self.prior_bar is None or self.prior_bar.date != self.date:
            self.day_travel = self.travel
            self.day_volume = b['volume']
            self.day_notional = notional
            self.day_msg_count = b['count']
        else:
            self.day_travel = self.prior_bar.day_travel + (b['high'] - b['open']) + (b['high'] - b['low']) + b['close'] - b['low']
            self.day_notional = self.prior_bar.day_notional + notional
            self.day_volume = self.prior_bar.day_volume + b['volume']
            self.day_msg_count = self.prior_bar.day_msg_count + plumbing.safe_value(b['count'], 0)
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
        self.day_vwap = plumbing.safe_divide(self.day_notional, self.day_volume, default=None)
        return self.get()


def test(symbol, date, bar_size):
    sql = '''select * from poly_bar where symbol='%s' and date='%s' order by time;''' % (symbol, date)

    pa = poly_bar.PolyBar()
    raw = pa.query(sql, pa.columns)
    
    bb = []
    bar =  None
    for b in raw:
        #print('b',b)
        bar = Bar(symbol, bar_size, prior_bar=bar)
        d = bar.post(b)
        bb += [bar]
        
    kk = ['time','start_time','end_time','am_count', 'bar_count','session','volume','pro_rata','new','complete','ret','skipped_am','skipped_full']
    pt = prettytable.PrettyTable(kk)
    for bar in bb:
        if bar.complete:
            d = bar.get()
            row = list(map(lambda k: d[k], kk))
            pt.add_row(row)
            
    print(pt)


def run_single(symbol, date, bar_size):
    #s = adjustor.get_adjustment_ratios([symbol], date, adjustment_date)
    sql = '''SELECT * 
             FROM poly_bar 
             WHERE date='%s' 
             AND symbol='%s' 
             ORDER BY time;''' % (date, symbol)
    pb = poly_bar.PolyBar()
    raw = pb.query(sql)
    bb = []
    bar =  None
    for b in raw:
        k = (b['symbol'], b['date'])
        #ratio = s.get(k, 1)
        bar = Bar(symbol, bar_size, prior_bar=bar)
        bar.post(b)
        bb += [bar]
    return bb

def run_multi(kk:list, bar_size:int=30) -> dict:
    '''
    where kk = [(symbol, date), ...]
    '''
    symbols = list(set(map(lambda k: k[0], kk)))
    results = {} #[(symbol, date)] = [] list of bars
    for symbol in symbols:
        sub = filter(lambda k: k[0] == symbol, kk)
        dd = sorted(list(set(map(lambda k: k[1], sub))))
        #s = adjustor.get_adjustment_ratios([symbol], min(dd), adjustment_date)
        sql = '''SELECT * 
                FROM poly_bar 
                WHERE symbol='%s' 
                AND date >= '%s' 
                AND date <= '%s'
                ORDER BY date,time;''' % (symbol, min(dd),max(dd))
        #print(sql)
        pb = poly_bar.PolyBar()
        raw = pb.query(sql)

        bar =  None
        bb = []
        for b in raw:
            if bar is not None and b['date'] > bar.date_dt:
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
    start = '20200831'
    #end = '20210107'
    bb = run_single(symbol, start, 60)
    for b in bb:
        print(b.get())