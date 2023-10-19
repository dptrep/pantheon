'''
fetch the right intraday bar

by: Dan Trepanier

Sep 22, 2022

BACKGROUND
    ib_minute   : 1 minute bars for VIX and SPY
    ib_trades   : 30 second bars
    poly_bar    : polygon historical 1 minute bars
    poly_am     : polygon real time AM 1 minute bars

TO DO:
    - resize bars on the fly?

'''

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import time
import copy
import prettytable

from src.core import plumbing
from src.apps.bar_maid.core import translator
from src.apps.bar_maid.intraday import availability
from src.apps.bar_maid.intraday import ib_minute
from src.apps.bar_maid.intraday import ib_am
from src.apps.bar_maid.intraday import ib_trades
from src.apps.bar_maid.intraday import poly_bar
from src.apps.bar_maid.intraday import poly_am
from src.apps.bar_maid.intraday import rebar


class Manager(object):
    def __init__(self, symbols:list, 
                       days:list):
        '''
        symbols : ['AAPL', ...]
        days    : ['20220901', ...]
        '''
        self.symbols = symbols
        self.days = days
        self.translator = translator.Translator()
        self.symbols = copy.deepcopy(self.translator.clean_list(symbols))
        self.availability = availability.Availability(self.symbols, self.days)
        self.data = {} # [(symbol, date)] = bb
        self.missing = []
        self.get_multi(symbols, days)
    
    def get(self, symbol:str, 
                  date:str, 
                  bar_size:int=1, 
                  rth_only:bool=False, 
                  complete_only:bool=False,
                  ) -> list:
        '''
        input:
            symbol          : 'AAPL','IBM',etc
            day             : 'yyyymmdd'
            bar_size        : 1, 5, 15, 30, 60 (minutes)
            rth_only        : only return regular trading hours
            complete_only   : only return bars that are complete
        output:
            when bar_size > 1, returns a list of bars closing at the minute increments
        '''
        s = self.translator.get(symbol, date)
        k = (s, date)
        if k not in self.data and k not in self.missing:
            self._fetch_single(s, date)
        bb = self.data.get(k, None)
        if bb is None:
            logging.warning('%s %s | has no data' % (s, date))
            self.missing += [k]
            return None
        elif bar_size > 1:
            bb = rebar.rebar(symbol, date, bb, bar_size * 60)
        if complete_only:
            bb = list(filter(lambda b: b.complete, bb))
        if rth_only:
            bb = list(filter(lambda b: b.session == 'day', bb))
        return bb

    def get_multi(self, symbols:list, 
                        days:list, 
                        sort_by:str='time', 
                        rth_only:bool=False, 
                        ):
        assert sort_by in ('symbol','time')
        kk = []
        for day in days:
            for symbol in symbols:
                s = self.translator.get(symbol, day)
                k = (s, day)
                if k not in self.data and k not in self.missing:
                    kk += [k]
        self._fetch_multi(kk)

        rpt = []
        for symbol in symbols:
            for d in days:
                s = self.translator.get(symbol,d)
                k = (s,d)
                r = self.data.get(k)
                if r is None:
                    if k not in self.missing:
                        self.missing += [k]
                else:
                    assert type(r) == list
                    for bar in r:
                        bar.symbol = symbol
                        rpt += [bar]
        if sort_by == 'symbol':
            rpt = sorted(rpt, key=lambda b: b.symbol)
        else:
            rpt = sorted(rpt, key=lambda b: b.datetime)
        
        return rpt

    def _fetch_single(self, symbol, date):
        start_t = time.time()
        switch = {'poly_am': poly_am.run_single,
                  'poly_bar': poly_bar.run_single,
                  'ib_trades': ib_trades.run_single,
                  'ib_minute': ib_minute.run_single,
                  'ib_am': ib_am.run_single,
                  }

        s = self.translator.get(symbol, date)
        source = self.availability.get(s, date)
        logging.info('%s %s | %s' % (date, symbol, source))
        if source is None:
            logging.warning('%s %s | no data available' % (date, symbol))
        else:
            data = switch[source](symbol, date, 60)
            end_t = time.time()
            logging.info('%s %6s | %8d items | %10s | dt=%8.6f s ' % (date, symbol, len(data), source,  (end_t-start_t)))
            self.data[(symbol, date)] = data
    
    def _fetch_multi(self, kk):
        '''
        input:
            kk : [('SPY', '20220102'), ...]
        '''
        switch = {'poly_am': poly_am.run_multi,
                  'poly_bar': poly_bar.run_multi,
                  'ib_trades': ib_trades.run_multi,
                  'ib_minute': ib_minute.run_multi,
                  'ib_am': ib_am.run_multi,
                  }
        d = self.availability.get_multi(kk)
        for (source, lst) in d.items():
            logging.info('%s | %d requesting | %s ' % (source, len(lst),lst))
            start_t = time.time()
            r = switch[source](lst, 60)
            end_t = time.time()
            logging.info('%s | %d requests | %d received | dt=%8.6f s ' % (source, len(lst), len(r), (end_t-start_t)))
            if len(self.data) == 0:
                self.data = r
            else:
                #print(self.data.keys())
                #print(r.keys())
                self.data = {**self.data, **r}

def test(symbols, days):
    m = Manager(symbols=symbols,
                days=days,
                bar_size=1800)

    rpt = m.get_multi(symbols, ['20220901'])
    kk = ['time','start_time','end_time','symbol', 'bar_count','session','volume','pro_rata','new','complete','ret','skipped_am','skipped_full']
    pt = prettytable.PrettyTable(kk)
    for bar in rpt:
        if bar.complete:
            d = bar.get()
            row = list(map(lambda k: d[k], kk))
            pt.add_row(row)
    print(pt)

    
if __name__ == '__main__':
    symbols = ['AAPL','SPY','VIX']
    days = plumbing.get_trading_days('20230801','20230806')
    m = Manager(symbols, days)
    # test single
    bb = m.get('VIX','20230804')
    print('VIX 20230804 bars:', len(bb))
    for b in bb:
        if b.session == 'day':
            print(b.time, b.close)
    print('---')
    exit(0)
    # test multiple days
    bars = m.get_multi(['VIX'], days)
    print('VIX 20230125 - 20230126')
    for bar in bars:
        if bar.session == 'day':
            print(bar.datetime,bar.symbol, bar.close)
    print('---')
    # test multiple symbols and days
    print('TWO symbols multiple days : AAPL,VIX')
    bars = m.get_multi(['AAPL','VIX'], days)
    for bar in bars:
        if bar.session == 'day':
            print(bar.datetime, bar.symbol, bar.close)
        
    print('---')
    print('adding a new symbol')
    bars = m.get_multi(['GOOG'], days)
    for bar in bars:
        print(bar.datetime,bar.symbol, bar.close)
    print('---')


    

    