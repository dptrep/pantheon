'''
day bars from interactive brokers

by: Dan Trepanier

Aug 2, 2022

Used mostly for VIX

Missing data for:

[('20060501', '20060519')] -- date range
TO_DO = [('20110912', '20110912'),]#

'''

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import argparse
import sys
import os
import time
from matplotlib.pyplot import table

from src.ib import wrapper, client, contract
from src.ib.utils import iswrapper #just for decorator
from src.ib.common import BarData
from src import settings
#from src.core import luigi_core
from src.apps.operations.workers import luigi
from src.core import plumbing, ib_core
 
from src.apps.market_data.models import contracts
from src.apps.market_data.models import ib_minute
#from src.apps.market_data.views import big_vx

START = '20051003'

class TestWrapper(wrapper.EWrapper):
    def __init__(self, on_completion_callback, on_error_callback, on_update_callback=None):
        self.results = {}
        
        self.on_completion_callback = on_completion_callback
        self.on_error_callback = on_error_callback
        self.on_update_callback = on_update_callback
        self.errors = []
        self.date = None
    
    @iswrapper
    def historicalData(self, reqId:int, bar: BarData):
        print("HistoricalData. ", reqId, " Date:", bar.date, "Open:", bar.open,
                "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
                "Count:", bar.barCount, "WAP:", bar.wap)
        if bar.date[:8] != self.date:
            self.date = bar.date[:8] 
            logging.info('received: %s' % (self.date))
        if reqId not in self.results:
            self.results[reqId] = []
        self.results[reqId] += [bar]
        if self.on_update_callback is not None:
            self.on_update_callback(bar, new=False, req_id=reqId)

    @ iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        logging.info('%d -- found %d lines from %s to %s' % (reqId, len(self.results[reqId]), start, end))
        self.on_completion_callback(reqId, self.results[reqId])
        del self.results[reqId]
    
    @ iswrapper
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        """returns updates in real time when keepUpToDate is set to True"""
        if self.on_update_callback is not None:
            self.on_update_callback(bar, new=True, req_id=reqId)
    
    @ iswrapper
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str=""):
        #reqId, errorCode, errorString, advancedOrderRejectJson
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        print("Id: ", reqId, "Error Code: ", errorCode, "Msg: ", errorString, 'json',advancedOrderRejectJson)
        if errorCode > 0 and self.on_error_callback is not None:
            self.errors += [(reqId, errorCode)]
            if len(self.errors) > 100:
                logging.warning('too many errors: %s' % str(self.errors))
                exit(1)
            self.on_error_callback(reqId, errorCode, errorString)
        
class TestClient(client.EClient):
    def __init__(self, live, wrapper, client_id):
        client.EClient.__init__(self, wrapper)
        switch = {True:{'port': settings.PRODUCTION_PORT},
                  False: {'port': settings.PAPER_PORT}}
        self.connect(host=settings.IB_HOST, 
                     clientId=client_id,  
                     **switch[live])
        time.sleep(2)
        

class App(object):
    def __init__(self, client_id:int, 
                       to_do_list:list, 
                       on_update=None, 
                       live=True):
        self.to_do_list = to_do_list
        self.table_class = ib_minute.IbMinute()
        self.wrapper = TestWrapper(self.on_completion, self.on_error, on_update)
        self.client = TestClient(live, self.wrapper, client_id)
        self.symbol_count = 0
        self.reqId = 0
        self.requests = {}
        self.errors = []
        self.keep_up_to_date = on_update is not None
        self.count = 0
        
    def start(self):
        logging.info('processing: %d items on the to do list' % len(self.to_do_list))
        self.make_request()
        self.client.run()
        
    
    def end(self):
        logging.info('done')
        self.client.done = True
        self.client.disconnect()
    
    def make_request(self):
        t = plumbing.current_time(normal=False, time_unit='s', time_zone='EST')
        if self.reqId < len(self.to_do_list):
            x = self.to_do_list[self.reqId]
            logging.info('reqId: %d | %d requests| %s' % (self.reqId, len(self.to_do_list), x))
            switch = {True: '',
                      False: '%s 23:59:00' % x['end']}
            query_time = switch[self.keep_up_to_date]
            self.client.reqHistoricalData(self.reqId, x['contract'], query_time,
                                          x['lookback'], x['bar_size'], x['data_type'], 
                                          1, 1, self.keep_up_to_date, [])
            self.requests[self.reqId] = x
            self.reqId += 1
            
    def on_completion(self, reqId, result):
        logging.info('%d | on_completion' % reqId)
        r = self.requests[reqId]
        lst = []
        for b in result:
            k = (r['symbol'], r['con_id'], b.date)
            #if k not in self.available:
            if len(b.date) == 8:
                date = b.date
                time = None
            else:
                tmp = b.date.split(' ')
                t = tmp[1].split(':')
                date = tmp[0][:8]
                if 'America/Los_Angeles' in b.date:
                    if int(t[0]) < 21:
                        time = '%0d:%2s:%2s' % (int(t[0]) + 3, t[1], t[2])
                    else:
                        date = plumbing.get_next_trading_day(date)
                        time = '%0d:%2s:%2s' % (int(t[0]) + 3 - 24, t[1], t[2])
                else:
                    time =  '%0d:%2s:%2s' % (int(t[0]), t[1], t[2])
            x = {'date':date, 'time': time,
                    'con_id': r['contract'].conId,
                    'open': b.open, 'high':b.high, 'low':b.low, 'close': b.close,
                    'volume': b.volume, 'count': b.barCount,'average':b.wap
                    }
            row = dict(x, **r)
            #print('r:',row)
            lst += [row]
        logging.info('reqId %d just got done -- %s' % (reqId, r))

        if not self.keep_up_to_date:
            self.table_class.post(lst)
            self.count += len(lst)
        
            if self.reqId >= len(self.requests):
                logging.info('end of App requests: %d of %d' % (self.reqId, len(self.requests)))
                self.end()
            else:
                logging.info('making more requests: %d of %d' % (self.reqId, len(self.requests)))
                self.make_request()
    
    def on_error(self, reqId, errorCode, errorString):
        logging.warning('%d | on_error' % reqId)
        if reqId in self.requests:
            logging.warning('skipping %d: %s %s' % (reqId, errorString, self.requests[reqId]))
            self.errors += [dict(self.requests[reqId],** {'errorCode': errorCode,'errorString': errorString})]
            self.make_request()
    
    
def get_available():
    available = {}
    table_class = ib_minute.IbMinute()
    sql_stmt = '''select symbol, con_id, data_type, max(date)
                  from ib_minute
                  group by symbol,con_id, data_type;
                  '''
    logging.info(sql_stmt)    
    raw = table_class.query(sql_stmt, ['symbol','con_id','data_type','date'])
    for x in raw: 
        k = (x['symbol'], x['con_id'], x['data_type'])
        available[k] = x['date']
    return available

def get_ib_contracts(symbols:list) -> list:
    ss = ','.join(list(map(lambda s: "'%s'" % s, symbols)))
    c = contracts.Contracts()
    sql_stmt = '''select * 
                  from contracts 
                  where sec_type not in ('OPT','FUT') 
                  and symbol in (%s) 
                  order by symbol, sec_type;''' % ss
    raw = c.query(sql_stmt, c.columns)
    new = []
    for x in raw:
        empty_class = contract.Contract()
        cont = ib_core.populate_class(x, empty_class)
        cont.includeExpired = cont.secType == 'FUT'
        new += [cont]
    return new

def get_request(cont, data_type, start, end, bar_size='1 min'):
    count = max(1,len(plumbing.get_trading_days(start, end)))
    if count > 252: lookback = '%d Y' % int(count / 252 + .5)
    else:           lookback = '%d D' % count
    row = {'symbol':cont.localSymbol,'con_id':cont.conId,
           'contract': cont, 'data_type': data_type,
           'bar_size':bar_size,
           'start': start, 'end': end, 
           'count': count, 'lookback':lookback}
    logging.info(row)
    return row

def get_to_do_list(symbols:list, 
                    days:list, 
                    available:dict, 
                    data_type='TRADES'):
    '''
    available = {'symbol': max(date)}
    '''
    start = min(days)
    end = max(days)
    contract_list = get_ib_contracts(symbols)
    
    requests = []
    logging.info('fetching to do items from regular items: %d items' % len(contract_list))
    for cont in contract_list:
        if cont.localSymbol in available:
            d = available[cont.localSymbol]
            s = plumbing.get_next_trading_day(d)
        else:
            s = start
        req = get_request(cont, data_type, s, end)
        requests += [req]
    return requests

def run(symbols, days, available):
    #days = plumbing.get_trading_days(start, end)
    to_do_list = get_to_do_list(symbols, days, available)
    if len(to_do_list) == 0:
        logging.info('ib_minute already populated')
        return []        
    
    client_id = settings.CLIENT_IDs['ib_bar']
    logging.info('ib_minute requesting %d days from %s to %s' % (len(days),min(days),max(days)))
    a = App(client_id, to_do_list)
    a.start()
    return a

def main(initialize=False, symbols=['VIX']):
    available = get_available() # d = {k:date}  -- k = (x['symbol'], x['con_id'], x['data_type'])
    last_day = max(available.values())
    start = plumbing.get_next_trading_day(last_day)
    end = plumbing.get_most_recent_day(cutoff=21*3600)
    if last_day >= start:
        logging.warning('no days to process -- last populated date: %s' % last_day)
        return 1
    
    all_days = plumbing.get_trading_days(start, end)
    
    if len(all_days) == 0:
        logging.warning('no days to process -- last populated date: %s' % last_day)
        return 1
    else:
        logging.info('IB minute requesting (%d) days: %s to %s' % (len(all_days), min(all_days), max(all_days)))
        a = run(symbols, all_days, available)
        n = a.count
        new_max_date = a.table_class.get_max_date()
        full_file = os.path.abspath(__file__)
        luigi.post(full_file=full_file, 
                request={'date': new_max_date}, 
                result= {'n':n},
                done=n>0) 


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol','--symbols', help="symbol or symbol list comma separated",type=str, default=None)
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    if args.symbol is None:
        symbols = ['VIX']
    else:
        symbols = args.symbol.split(',')
    main(symbols=symbols)
    logging.info('done')