'''
serves up historical bars

by: Dan Trepanier

Sep 20, 2022
'''

import logging
#import datetime
import copy
#from multiprocessing import Process

from src.core import plumbing

from src.apps.bar_maid.intraday import manager as intra_manager
from src.apps.bar_maid.daily import manager as daily_manager
from src.apps.bar_maid.intraday import rebar

class DataFarm(object):
    def __init__(self, symbols:list, 
                       days:list, 
                       data_type:str='day_raw'):
        '''
        symbols          : list of symbols
        days             : list of 'yyyymmdd',...
        data_type        : {'day_raw', 'AM'}
        populate_missing : populate missing data (not working yet)
        '''
        if data_type in ('poly_am'):
            data_type = 'AM'
        elif data_type in ('daily'):
            data_type = 'day_raw'
        assert data_type in ('day_raw','AM')
        self.symbols = sorted(list(set(symbols)))
        self.days = days
        self.data_type = data_type
        self.today = plumbing.today()
        assert len(days) ==0 or max(days) <= self.today,'max date is in the future: %s' % max(days)
        
        self.switch = {'day_raw': daily_manager.Manager,
                        'AM': intra_manager.Manager}
        self.manager = self.switch[data_type](symbols, days)
        
    def get(self, symbol:str, 
                  day:str, 
                  bar_size:int=1, 
                  complete:bool=False):
        '''
        input:
            symbol          : 'AAPL','IBM',etc
            day             : 'yyyymmdd'
            bar_size        : 1, 5, 15, 30, 60 (minutes)
            complete_only   : only return bars that are complete
        output:
            when bar_size > 1, returns a list of bars closing at the minute increments
        '''
        bb = self.manager.get(symbol, day)
        if bb is None:
            return bb
        elif self.data_type == 'AM':
            if bar_size > 1:
                bb = rebar.rebar(symbol, day, bb, bar_size * 60)
            if complete:
                bb = list(filter(lambda b: b.complete, bb))
            return bb
        elif self.data_type == 'day_raw':
            if bar_size > 1:
                logging.warning('%s | %s with bar size %s not implimented for data type %s' % (symbol, day, bar_size, self.data_type))
        return bb
        
    def get_multi(self, symbols:list, 
                        days:list, 
                        sort_by:str='time',
                        bar_size:int=1,
                        complete:bool=False) -> list:
        '''
        input:
            symbols  : ['AAPL', ...]
            days     : ['yyyymmdd', ...]
            sort_by  : 'symbol' or 'time'
            bar_size : 1, 5, 15, 30, 60 (minutes)
        output:
            when bar_size > 1, returns a list of bars closing at the minute increments
        '''
        assert sort_by in ('symbol','time')
        
        if bar_size == 1:
            bb = self.manager.get_multi(symbols, days, sort_by)
            return bb
        else:
            if self.data_type != 'AM':
                bb = self.manager.get_multi(symbols, days, sort_by)
                logging.warning('bar size %s not implimented for data_type: %s' % (bar_size, self.data_type))
                return bb
            else:
                rpt = []
                for symbol in symbols:
                    for day in days:
                        bb = self.get(symbol, day, bar_size, complete)
                        rpt += bb
                if sort_by == 'symbol':
                    return sorted(rpt, key=lambda b: b.symbol)
                else:
                    return sorted(rpt, key=lambda b: b.datetime)
                    

    