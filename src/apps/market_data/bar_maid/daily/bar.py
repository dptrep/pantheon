'''
Daily Bar

by: Dan Trepanier

Sept 23, 2022
'''

import logging
import json

from src.core import plumbing

class Bar(object):
    def __init__(self, symbol:str, 
                       prior_bar=None):
        '''
        symbol    :   i.e. AAPL
        prior_bar :   IntradayBar object
        '''
        self.symbol = symbol
        self.prior_bar = prior_bar
        self.bar_size = 'D'

        # symbol information
        self.con_id = None

        # ib sequence number
        self.sequence_number = None # am bar seqence number
        self.session = None # session in ('pre','day','after)

        # datetime metrics
        self.date = None
        self.date_dt = None
        self.time = None # datetime.time
        self.closing_time = None # datetime.time
        self.datetime = None # datetime.datetime timestamps (time of sample)
        
        # metrics measured in seconds
        self.t = None # seconds from midnight where sample is taken
        self.closing_t = None
        
        # bar price metrics
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.vwap = None # weighted average price

        # bar volume / activity
        self.avg_size  = 0 # average trade size
        self.volume = 0 # volume traded
        self.notional = 0 # notional value traded
        self.msg_count = 0 # number of messages in bar
        
        # bar completion metrics
        self.complete = None
        self.pro_rata = None # this is equal to 1.0 at the end of a bar -- 0 before 09:30:00 -- 1.0 at close
        
        self.skipped_full = None # whether we skipped an empty bar
        
        self.duplicate = None # duplicate bar for this timestamp -- should ignore these upstream
        
        # relative to last closed bar -- None when last bar is not provided
        self.last_close = None
        self.ret = None

        # corporate actions
        self.before = 1
        self.after = 1
        self.dividend = 0

        self.data_source = None # choices: ['day_raw','ib_trades', 'poly_bar', 'poly_am']

        # adjustment radio
        self.ratio = 1

    def post_con_id(self, con_id):
        self.con_id = con_id
    
    def close_out(self):
        self.complete = True

    def get(self, adjust=False, gross_up=False):
        '''
        adjust   : for splits
        gross_up : gross_up for the full day's worth of volume, notional, msg_count
        '''

        kk = ['symbol',
              'con_id',
              'bar_size',
              'sequence_number',
              'session',

              'datetime',
              'date',
              'date_dt',
              'time','end_time',
              
              't','end_t',
              'pro_rata',
              
              'notional',
              
              'complete','duplicate','skipped_full',
            
              'ret',
              'data_source',
              'session',
              'dividend',
              'before',
              'after',
              'ratio']

        kk_multiply   = ['open','high','low','close','vwap',
                      'last_close',]
        kk_divide = ['volume', 'avg_size', 'msg_count']
        
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
              'time','end_time',]
        for k in kk:
            r[k] = str(r[k])
        return json.dumps(r)