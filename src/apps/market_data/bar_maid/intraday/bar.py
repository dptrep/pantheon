'''
bar record

standard bar record to be used to generate dictionary

by: Dan Trepanier

Sep 21, 2022

TO DO:
    - should provide a "gross_up" option to gross_up volume / notional of larger bars

'''
import logging
import copy
import json

from src.core import plumbing


class Bar(object):
    def __init__(self, symbol:str, 
                       bar_size:int=None,
                       prior_bar=None):
        '''
        symbol    :   i.e. AAPL
        bar_size  :   in seconds | None is a day bar
        prior_bar :   IntradayBar object
        '''
        self.symbol = symbol
        self.prior_bar = prior_bar
        
        # symbol information
        self.con_id = None

        # ib sequence number
        self.sequence_number = None # am bar seqence number
        self.session = None # session in ('pre','day','after)

        # datetime metrics
        self.date = None # str
        self.date_dt = None # datetime.date
        self.time = None
        self.datetime = None # datetime.datetime timestamps (time of sample)
        self.start_time = None # bar start time
        self.end_time = None # bar end time 
        
        # metrics measured in seconds
        self.bar_size = bar_size
        self.t = None # seconds from midnight where sample is taken
        self.start_t = None 
        self.end_t = None
        
        # bar price metrics
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.travel = 0 # open to high, to low, to close
        self.vwap = None # weighted average price

        # bar volume / activity
        self.avg_size  = 0 # average trade size
        self.volume = 0 # volume traded
        self.notional = 0 # notional value traded
        self.msg_count = 0 # number of messages in bar
        
        self.am_count = 0 # equivalent to seconds from the open -- 0 index is 09:30:00 to 09:31:00
        self.bar_count = 0 # 09:30:00 is start time of bar 0
        
        # bar completion metrics
        self.complete = None
        self.pro_rata = None # this is equal to 1.0 at the end of a bar
        self.skipped_full = None # whether we skipped an empty bar
        self.skipped_am = None  # whether we skipped an empty bar AM bar -- same as skipped_full when bar_size = 60
        self.duplicate = None # duplicate bar for this timestamp -- should ignore these upstream
        self.new = None # start of a new bar

        # day time metrics
        self.day_open = None
        self.day_vwap = None
        self.day_volume = 0
        self.day_notional = 0
        self.day_msg_count = 0
        self.day_high = None
        self.day_low = None
        self.day_travel = 0

        # relative to last closed bar -- None when last bar is not provided
        self.last_close = None
        self.ret = None

        self.data_source = None # choices: ['ib_trades', 'poly_bar', 'poly_am','live']

        # adjustment radio
        self.ratio = 1

    def post_ratio(self, ratio):
        self.ratio = ratio

    def close_out(self):
        self.complete = True

    def get(self, adjust=False, gross_up=False):
        '''
        adjust   : for splits
        gross_up : gross_up for the full day's worth of volume, notional, msg_count
        '''
        kk = ['symbol',
              'con_id',
              'sequence_number',
              'datetime',
              'date',
              'date_dt',
              'time','start_time','end_time',
              't','start_t','end_t',
              'bar_size','pro_rata',
              
              'notional',
              'day_notional',

              'msg_count', 'day_msg_count',

              'am_count','bar_count',
              'complete','duplicate','new','skipped_am','skipped_full',
            
              'ret',
              'data_source',
              'session',
              'ratio']

        kk_multiply   = ['open','high','low','close','vwap','travel',
                       'last_close',
                       'day_open','day_high','day_low','day_vwap','day_travel']
        kk_divide = ['volume', 'avg_size', 
                       'day_volume',]

        r = {}
        if adjust and self.ratio != 1:
            for k in kk_divide:
                v = getattr(self, k)
                r[k] = plumbing.safe_divide(v, self.ratio, default=None)
            for k in kk_multiply:
                v = getattr(self, k)
                r[k] = plumbing.safe_multiply(v, self.ratio, default=None)
        else:
            kk += kk_divide + kk_multiply
        
        for k in kk:
            r[k] = getattr(self, k)    
        return r

    def get_json(self, adjust=False):
        '''
        adjust for splits : 
                should not be necessary for JSON since 
                this happens in real time where current price 
                is the relative price we care about
        '''
        r = self.get(adjust)

        kk = ['datetime','date_dt',
              'time','start_time','end_time',]
        for k in kk:
            r[k] = str(r[k])
        return json.dumps(r)

