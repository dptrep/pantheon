'''
daily bar manager

by: Dan Trepanier

Sep 23, 2022
'''
import logging
import copy
import time

from src.core import plumbing
from src.apps.bar_maid.core import translator
from src.apps.bar_maid.daily import day_raw
from src.apps.bar_maid.daily import day_ib
from src.apps.bar_maid.daily import bar

class Manager(object):
    def __init__(self, symbols, days):
        self.symbols = symbols
        self.today = plumbing.today()
        self.days = days
        self.last_day = plumbing.get_most_recent_day()
        self.translator = translator.Translator()
        self.symbols = copy.deepcopy(self.translator.clean_list(symbols))
        self.data = {}
        self.missing = []
        self._fetch_multi(symbols, days)
    
    def get(self, symbol:str, date:str) -> bar.Bar:
        s = self.translator.get(symbol, date)
        k = (s, date)
        if k not in self.data and k not in self.missing:
            self._fetch_single(s, date)
        return self.data.get(k, None)

    def get_multi(self, symbols:list, days:list, sort_by:str='time')->list:
        assert sort_by in ('symbol','time')
        self._fetch_multi(symbols, days)
        rpt = []
        for symbol in symbols:
            for d in days:
                s = self.translator.get(symbol,d)
                k = (s,d)
                bar = self.data.get(k)
                if bar is not None:
                    bar.symbol = symbol
                    rpt += [bar]
        if sort_by == 'symbol':
            return sorted(rpt, key=lambda b: b.symbol)
        else:
            return sorted(rpt, key=lambda b: b.datetime)

    def _refresh_universe(self, symbols, days):
        self.symbols = sorted(list(set(self.symbols) | set(symbols)))
        dd = self.days + days
        end = min(self.last_day, max(dd))
        start = min(dd + [end])
        self.days = plumbing.get_trading_days(start, end)

    def _fetch_single(self, symbol, date):
        self._refresh_universe([symbol], [date])
        start_t = time.time()
        if symbol == 'VIX':
            raw = day_ib.run_single(symbol, self.days)
        else:
            raw = day_raw.run_single(symbol, self.days)
        dd = []
        for bar in raw:
            k = (bar.symbol, bar.date)
            dd += [bar.date]
            if k not in self.data:
                self.data[k] = bar
        
        missing = list(set(self.days) - set(dd))
        for d in missing:
            self.missing += [(symbol, d)]
        end_t = time.time()
        logging.info('%s %6s | %8d items | %10s | dt=%8.6f s ' % (date, symbol, len(raw), 'day_raw',  (end_t-start_t)))

    def _fetch_multi(self, symbols, days):
        kk = []
        ss = []
        dd = []
        for symbol in symbols:
            for day in days:
                s = self.translator.get(symbol, day)
                k = (s, day)
                if k not in self.data and k not in self.missing:
                    kk += [k]
                    ss += [s]
                    dd += [day]
        ss = sorted(list(set(ss)))
        dd = sorted(list(set(dd)))
        self._refresh_universe(ss, dd)
        if len(ss) > 0 and len(dd) > 0:
            dd = plumbing.get_trading_days(min(self.days), max(self.days))
            start_t = time.time()
            new = day_raw.run_multi(ss, dd)
            if 'VIX' in ss:
                vix = day_ib.run_multi(['VIX'], dd)
                new = {**new, **vix}
            end_t = time.time()
            logging.info('symbols=%6d | days=%6d | %8d items | %10s | dt=%8.6f s ' % (len(ss), len(dd), len(new), 'day_raw',  (end_t-start_t)))
            missing = list(set(kk) - set(new.keys()))
            self.missing += missing
            #self.data = self.data |  new
            self.data = {**self.data, **new}


def test():
    symbols = ['AAPL','SPY','VIX']
    days = plumbing.get_trading_days('20221004','20221005')
    m = Manager(symbols, days)
    # test single
    d = days[0]
    print('VIX %s' % d)
    b = m.get('VIX',d)
    print(b.get())
    print('---')
    d = days[-1]
    print('AAPL %s' % d)
    b = m.get('AAPL',d)
    print(b.get())
    print('---')

    # test multiple days
    bars = m.get_multi(['AAPL','VIX'], days)
    print('AAPL %s - %s' % (min(days), max(days)))
    for bar in bars:
        print(bar.date, bar.close)
    print('---')
    # test multiple symbols and days
    '''
    print('TWO symbols multiple days : AAPL,SPY')
    bars = m.get_multi(['AAPL','SPY'], days)
    for bar in bars:
        print(bar.date,bar.symbol, bar.close)
    print('---')
    print('adding a new symbol')
    bars = m.get_multi(['GOOG'], days)
    for bar in bars:
        print(bar.date,bar.symbol, bar.close)
    print('---')
    '''    
if __name__ == '__main__':
    test()