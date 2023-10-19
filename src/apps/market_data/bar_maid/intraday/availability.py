'''
figure out data source for intraday bars

by: Dan Trepanier

Sept 22, 2022
'''
import logging
from src.core import plumbing
from src.apps.market_data.views import bar_avail

class Availability(object):
    def __init__(self, symbols:list, days:list) -> None:
        self.symbols = []
        self.days = []
        self.today = plumbing.today()
        self.recent = plumbing.get_last_trading_days(self.today, 5) + [self.today]
        self.not_available = set([])
        self.available = {} # [(symbol, date)]
        self.refresh(symbols, days)
        
    def get(self, symbol:str, date:str) -> str:
        k = (symbol, date)
        self.refresh([symbol], [date])
        if k in self.available:
            return self.available[k]
        elif date in self.recent:
            return 'poly_am'
        else:
            return self.available.get(k, None)

    def get_multi(self, kk:list) -> dict:
        '''
        kk = [(symbol, date), ...]
        returns:
        d['poly_am'] = [k, k]
        '''
        symbols = list(set(map(lambda k: k[0], kk)))
        days = list(set(map(lambda k: k[1], kk)))
        self.refresh(symbols, days)
        d = {}
        missing = []
        for k in kk:
            if k not in self.not_available:
                source = self.get(*k)
                if source is None:
                    missing += [k]
                elif source not in d:
                    d[source] = []
                if source is not None:
                    d[source] += [k]
        missing = set(kk) - set(d.keys())
        if len(missing) > 0:
            self.not_available = self.not_available | set(missing)
        return d

    def refresh(self, symbols:list=[], days:list=[]) -> None:
        new_ss = set(symbols) - set(self.symbols)
        new_dd = set(days) - set(self.days)
        if len(new_ss) > 0 or len(new_dd) > 0:
            logging.debug('looking for more availability data | new_ss: %s | new_dd: %s' % (new_ss, new_dd))
            self.symbols = list(set(self.symbols) | new_ss)
            self.days = list(set(self.days) | new_dd)
            if len(self.days) > 0:
                self.available = bar_avail.get_priority(min(self.days), max(self.days), self.symbols)
