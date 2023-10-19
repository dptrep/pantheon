'''
take a standard one minute bar dataset and resample to:
    - whatever bar_size

by: Dan Trepanier

Sep 23, 2022
'''

import logging
import datetime
import prettytable

from src.core import plumbing
from src.apps.bar_maid.intraday import bar
from src.apps.bar_maid.intraday import poly_am
#from src.apps.bar_maid.intraday import ib_trades
#from src.apps.bar_maid.intraday import poly_bar

class Bar(bar.Bar):
    def __init__(self, symbol, bar_size, prior_bar):
        assert bar_size >= 60, 'this cannot be used to shrink bar sizes: %s' % bar_size
        bar.Bar.__init__(self, symbol, bar_size, prior_bar)

    def post(self, b:dict) -> dict:
        sames = ['symbol',
                'con_id',
                'sequence_number',
                
                'datetime',
                'date','date_dt',
                'time','t',
                
                'day_open','day_high','day_low','day_vwap','day_travel',
                'day_volume','day_notional',
                
                'close',

                'am_count','skipped_am',
                'data_source','session']
        
        for k in sames:
            setattr(self, k, b[k])

        self.bar_count = int((self.t - 60 - 34200) / self.bar_size)
        if self.t <= 34200:
            self.bar_count -= 1

        self.start_t = self.bar_count * self.bar_size + 34200
        self.end_t = self.start_t + self.bar_size

        self.start_time = plumbing.normal_time(self.start_t, time_unit='s', fmt='time')
        self.end_time = plumbing.normal_time(self.end_t, time_unit='s', fmt='time')

        self.pro_rata = (self.t - self.start_t) / self.bar_size
        
        self.new = self.pro_rata <= (60 / self.bar_size + .001) # start of new bar
        self.complete = self.pro_rata > .999999 # end of bar
        #notional = plumbing.safe_multiply(b['vwap'], b['volume'], default=0)

        if self.bar_size == 60 or self.new or self.prior_bar is None:
            kk = ['open', 'high', 'low', 
                  'volume', 'vwap', 'avg_size',
                  'msg_count',
                  'travel',
                  'notional']
            for k in kk:
                setattr(self, k, b[k])
            
        else:
            self.open = self.prior_bar.open
            self.high = plumbing.safe_max([self.prior_bar.high, b['high']])
            self.low = plumbing.safe_min([self.prior_bar.low, b['low']])
            
            if not self.duplicate:
                kk = ['volume',
                      'notional',
                      'msg_count',
                      'travel']
                for k in kk:
                    last = getattr(self.prior_bar, k)
                    total = plumbing.safe_sum([last, b[k]], default=None)
                    setattr(self, k, total)
                self.avg_size = plumbing.safe_divide(self.volume, self.msg_count, default=None)
                self.vwap = plumbing.safe_divide(self.notional, self.volume, default=None)
                
        if self.prior_bar is not None:
            self.duplicate = self.prior_bar.datetime == self.datetime
            if self.prior_bar.date == self.date:
                self.skipped_full = self.bar_count - self.prior_bar.bar_count - 1
                if self.bar_count > self.prior_bar.bar_count:
                    self.prior_bar.close_out()
            else:
                self.skipped_full = 0
                self.prior_bar.complete = True

            if self.prior_bar.complete:
                self.last_close = self.prior_bar.close
            else:
                self.last_close = self.prior_bar.last_close

            if self.last_close is not None:
                self.ret = plumbing.safe_divide(10000 * (self.close - self.last_close), self.last_close, default=None)
            assert self.datetime >= self.prior_bar.datetime,'out of sequence bars\nlast: %s\nnew: %s' % (self.prior_bar.get(), self.get())

        
def rebar(symbol:str, 
          date:str, 
          bars:list, 
          bar_size:int, 
          rth_only_old:bool=False, 
          rth_only_today:bool=False):
    
    assert bar_size >= 60
    new = []
    big_bar = None
    for bar in bars:
        assert bar.symbol == symbol,'unexpected symbol %s vs expected: %s' % (bar.symbol, symbol)
        if bar.date == date:
            # today's data -- filter for trading hours
            if (rth_only_today and bar.session == 'day') or rth_only_today == False:
                b = bar.get()
            else:
                b = None
        else:
            assert bar.date < date,'date (%s) is in the future relative to the requested date: %s' % (bar.date, date)
            if (rth_only_old and bar.session == 'day') or rth_only_old == False:
                b = bar.get()
            else:
                b = None
        if b is not None:
            big_bar = Bar(symbol, bar_size, big_bar)
            big_bar.post(b)
            new += [big_bar]
    return new


def test(symbol, date, bar_size, rth_only_today):
    bb = poly_am.run_single(symbol, date, 60, None)
    print('raw bars', len(bb))
    new = rebar(symbol, date, bb, bar_size,rth_only_today=rth_only_today)
    print('new', len(new))
    
    kk = ['time','start_time','end_time','am_count', 'bar_count','session','volume','pro_rata','new','complete','ret','skipped_am','skipped_full']
    pt = prettytable.PrettyTable(kk)
    for bar in new:
        if bar.complete:
            d = bar.get()
            row = list(map(lambda k: d[k], kk))
            pt.add_row(row)
            
    print(pt)

def test_2(symbol, start, end, bar_size):
    days = plumbing.get_trading_days(start, end)
    kk = []
    for day in days:
        kk += [(symbol, day)]

    results = poly_am.run_multi(kk, bar_size=60)
    bb = []
    for lst in results.values():
        bb += lst

    print('raw bars', len(bb))
    new = rebar(symbol, end, bb, 
                bar_size,
                rth_only_today=True,
                rth_only_old=True)
    print('new', len(new))
    
    kk = ['date','time','start_time','end_time','am_count', 'bar_count','session','volume','complete','ret',]
    pt = prettytable.PrettyTable(kk)
    for bar in new:
        if bar.complete:
            d = bar.get()
            row = list(map(lambda k: d[k], kk))
            pt.add_row(row)
            
    print(pt)

if __name__ == '__main__':
    #test('AAPL','20220920', 600,True)
    test_2('AAPL', '20220920','20220922', 1800)