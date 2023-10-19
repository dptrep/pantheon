'''
Bar Maid

A simple app to serve up bars from a database.

Bars are cached locally in a directory.

by: Dan Trepanier
date: Sept 24, 2023
'''
import logging
import os
import pickle

from src.apps import settings
from src.core import plumbing

from src.apps.bar_maid.intraday import manager as intra_manager
from src.apps.bar_maid.daily import manager as daily_manager
from src.apps.bar_maid import data_farm

class BarMaid(object):
    def __init__(self, 
                 symbols, 
                 days,
                 data_type='I') -> None:
        '''
        symbols          : list of symbols
        days             : list of 'yyyymmdd',...
        data_type        : {'D', 'I'} or {'daily', 'intraday'}
        populate_missing : populate missing data (not working yet)
        '''
        self.symbols = symbols
        self.days = days
        self.data_type = data_type
        self.cache = {} # [(symbol, day)]
        self.df_am = data_farm.DataFarm(symbols=self.symbols,
                                        days=[], 
                                        data_type=data_type)
        self.location = settings.DIR + 'data/%s_bars/' % self.data_type
        
        if os.path.exists(self.location) is False:
            os.mkdir(self.location)
        
        self.files = os.listdir(self.location)
        self.load()
    
    def load(self):
        for symbol in self.symbols:
            for day in self.days:
                if (symbol, day) not in self.cache:
                    self.cache[(symbol, day)] = self._load(symbol, day)
    
    def _load(self, symbol, day):
        file_name = '%s_%s.pkl' % (symbol, day)
        if file_name in self.files:
            logging.info('loading %s' % file_name)
            return pickle.load(open(self.location + file_name, 'rb'))
        else:
            logging.info('fetching %s' % file_name)
            bb = self.df_am.get(symbol=symbol, day=day)
            pickle.dump(bb, open(self.location + file_name, 'wb'))
            return bb
        
    def get(self, symbol, day):
        k = (symbol, day)
        if k not in self.cache:
            self.cache[k] = self._load(symbol, day)
        return self.cache[k]
    
    def get_multi(self, symbols, days, sort_by:str='time') -> list:
        rpt = []
        for symbol in symbols:
            for day in days:
                rpt += self.get(symbol, day)
        if sort_by == 'symbol':
            return sorted(rpt, key=lambda b: b.symbol)
        else:
            return sorted(rpt, key=lambda b: b.datetime)
        
    
if __name__ == '__main__':
    bm = BarMaid(symbols=['SPY'],
                 days=['20230911'])
    
    bb =bm.get(symbol='SPY', day='20230911')
    bb =bm.get(symbol='SPY', day='20230918')
    for b in bb:
        print(b.time, b.datetime, b.close, b.ret)