'''
Created on Apr 22, 2022

@author: dan trepanier

poly SIP data collection
collects subscriptions specified in poly_sub

Channels:
    SIP.AAPL.T
    SIP.AAPL.Q
    SIP.AAPL.AM

To Do:
    [] add try and except loop with an email message

'''


import logging
from src import settings
log_file = settings.get_log_file(prefix='poly')
#print('log_file', log_file)
logging.basicConfig(level=logging.INFO,
                    #filename=log_file, 
                    format='%(asctime)s - MAIN - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(message)s')
import argparse
import datetime
import time
import polygon
import pytz
import redis
import json

from src.core import plumbing, mail
from src.core import luigi_core
from src.apps.market_data.views import validate
from src.apps.platform.models import poly_quotes_two, poly_trades_two, poly_am
from src.apps.platform.models import poly_sub

#from src.apps.radio.scripts import bod

def get_date_time(ts):
    d = datetime.datetime.fromtimestamp(ts / 1000.0) + datetime.timedelta(hours=3)
    date = '%d%02d%02d' % (d.year, d.month, d.day)
    time = '%02d:%02d:%02d' % (d.hour, d.minute, d.second)
    return (date, time, d.microsecond)

def parse_sip(t, x):
    msg = {'posting_time': '%02d:%02d:%02d' % (t.hour + 3, t.minute , t.second),
            'posting_micro': int(t.microsecond),
            'symbol':x['sym'],
            'msg': json.dumps(x)}
    return msg
    
def parse_am(t, x):
    try:
        date, start_time, _ = get_date_time(x['s'])
        date, end_time, _ = get_date_time(x['e'])
        msg = {'date':date,
                'posting_time': '%02d:%02d:%02d' % (t.hour + 3, t.minute , t.second),
                'posting_micro': int(t.microsecond),
                'start_time':start_time, 
                'end_time':end_time, 
                'symbol':x['sym'],
                'day_open':x.get('op',None),'day_vwap':x['a'],'day_volume':x['av'],
                'open':x['o'],'high':x['h'],'low':x['l'],'close':x['c'],
                'volume':x['v'],
                'vwap':x['vw'],'avg_size':x['z']
                }
        return msg
    except:
        logging.warning('unable to parse am: %s' % x)
        return None


class Poly(object):
    def __init__(self, events=['T','AM'], exclude=['HCMC']):
        self.exclude = settings.POLY_EXCLUDE + exclude
        self.date = plumbing.today()
        self.deadline = self.get_deadline()

        self.events = events
        self.switch = {'Q': parse_sip,
                       'T': parse_sip,
                       'AM': parse_am}
        self.sequence_number = self.get_sequence_number()
        self.subs = False
        self.redis = redis.Redis(**settings.REDIS_CONF_SIMS)
        self.pipe = self.redis.pipeline()
        
        self.count = 0
        self.last_time = datetime.datetime.now()
        
        self.batch_size = 10000
        self.exec_latency = 500

        self.connection_count = 0
        self.client = self.fetch_client()
    
    def get_deadline(self):
        local = pytz.timezone("America/Los_Angeles")
        naive = datetime.datetime.strptime(self.date + " 17:00:00", "%Y%m%d %H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        logging.info('%s is today -- deadline is UTC: %s' % (self.date, utc_dt))
        return int(utc_dt.timestamp())

    def fetch_client(self):
        self.connection_count += 1
        logging.info('creating websocket for stocks: %d count' % self.connection_count)
        client = polygon.WebSocketClient('stocks', 
                                         settings.POLYGON_KEY,
                                         process_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close= self.on_close)
        
        return client

    def on_message(self, json_msg):
        lst = json.loads(json_msg)
        t = datetime.datetime.now()
        today = str(t)[:10].replace('-','')
        if today != self.date:
            self.sequence_number = 0
            self.date= today
        for n,x in enumerate(lst):
            #print('on',n,x)
            if 'message' in x and not self.subs:
                self.subs = True
                self.subscribe()
            elif x['ev'] in self.events:
                self.sequence_number += 1
                self.count += 1
                self.post(t, x)
        dt = t - self.last_time
        if self.count > self.batch_size or (dt.seconds >= 1 and self.count > 10):
            self.last_time = t
            start_time = time.time()
            self.pipe.execute() 
            end_time = time.time()
            dt_red = (end_time - start_time) * 1e6

            exec_latency = dt_red / self.count
            self.exec_latency = .9 * self.exec_latency + .1 * exec_latency

            logging.info('%d posting to redis | %d items | dt_red: %d us (vs avg=%d) | dt_batch: %s' % (self.sequence_number, self.count, dt_red, self.exec_latency, dt))
            self.count = 0
            if end_time > self.deadline:
                self.on_close()

    def post(self, t, x):
        switch = {'Q': parse_sip,
                  'T': parse_sip,
                  'AM': parse_am}
        func = switch[x['ev']]
        msg = func(t, x)
        msg['sequence_number'] = self.sequence_number
        msg['msg_count'] = self.count
        content = json.dumps(msg).replace(' ','')
        cmd = "PUBLISH SIP.%s.%s %s" % (x['sym'], x['ev'], content)
        self.pipe.execute_command(cmd)

    def on_error(self, ws, error):
        # error is of type: <class 'AssertionError'>
        # unknown symbol : {'id': 931, 'date': '20211222', 'time': datetime.time(14, 9, 10), 'symbol': 'HCMC', 'event': 'T'}
        logging.warning('web socket error: %s' % error)
        time = plumbing.current_time(time_unit='s', time_zone='EST')
        if time >= 20 * 3600:
            self.pipe.execute() 
            logging.info('done for %s' % self.date)
            print(type(error), error)
            exit(1)
        elif 'Connection to remote host was lost' in str(error) and self.connection_count < 3:
            self.pipe.execute()
            logging.info('waiting 10 seconds')
            time.sleep(10)
            self.client = self.fetch_client()
            
    def on_close(self, *args, **kwargs):
        logging.info('done')
        print('args',args)
        print('kwargs',kwargs)
        exit(0)

    def subscribe_simple(self):
        lst = ['AM.*','T.*']
        logging.info('subscribing lst: %s' % lst)
        self.client.subscribe(*lst)

    def subscribe(self):
        ee = ','.join(list(map(lambda x: "'%s'" % x, self.events)))
        sql = '''SELECT * FROM poly_sub WHERE date='%s' and event in (%s);''' % (self.date, ee)
        logging.info(sql)
        ps = poly_sub.PolySub()
        raw = ps.query(sql, ps.columns)
        lst = []
        if 'AM' in self.events:
            lst += ['AM.*']
            logging.info('subscribing to %s' % lst[-1])
        
        if len(raw) == 0:
            raw = [{'symbol': 'SPY', 'event': 'T'}]
        
        symbols = list(map(lambda x: x['symbol'], raw))
        v = validate.validate_multi(symbols, self.date)
        for x in raw:
            if x['symbol'] in v:
                k = (x['symbol'], x['event'])
                k = '%s.%s' % (x['event'], x['symbol'])
                logging.info('subscribing to %s' % k)
                lst += [k]
            else:
                logging.warning('skipping sub to %s -- symbol not in universe' % x['symbol'])
        if len(lst) > 0:
            logging.info('subscribing lst: %s' % lst)
            self.client.subscribe(*lst)
    
    def get_sequence_number(self):
        raw = []
        if 'T' in self.events:
            sql = '''select max(sequence_number) from poly_t_%s;''' % self.date
            pt = poly_trades_two.PolyTradesTwo(self.date)
            raw += pt.query(sql, ['sequence_number'])    
        if 'Q' in self.events:
            sql = '''select max(sequence_number) from poly_q_%s;''' % self.date
            pq = poly_quotes_two.PolyQuotesTwo(self.date)
            raw += pq.query(sql, ['sequence_number'])
        if 'AM' in self.events:
            sql = '''select max(sequence_number) from poly_am_%s;''' % self.date
            pa = poly_am.PolyAM(self.date)
            raw = pa.query(sql, ['sequence_number'])
        
        rr = map(lambda x: x['sequence_number'], raw)
        rr = list(filter(lambda x: x is not None, rr))
        logging.info('sequence number: %s' % rr)
        if len(rr) == 0:
            return 0
        else:
            return max(rr)
    
    def start(self):
        self.client.run()

def poly(events):
    now = plumbing.current_time(time_unit='s', time_zone='EST')
    start_time = 6 * 3600
    dt = int(start_time - now)
    if dt > 0:
        logging.info('waiting until 6 am Eastern to start')
        time.sleep(dt)
    p = Poly(events)
    p.start()
    return p

def main(events):
    date = plumbing.today()
    trading_day = plumbing.is_trading_day(date)
    now = plumbing.current_time(time_unit='s', time_zone='EST')
    closing_t = plumbing.closing_time(date, time_unit='s', normal=False)
    if not (trading_day and now <  closing_t):
        logging.warning('date=%s | now=%d |  not trading day or too late in the day' % (date, now))
        return 1
    p = Poly(events)
    p.start()
    end_t = 3600 * 20
    
    while now < end_t:
        time.sleep(60)
        now = plumbing.current_time(time_unit='s', time_zone='EST')
        logging.info('POLY TWO | now=%6d vs end_time=%6d' % (now, end_t))
    
    logging.info('done')
    luigi_core.done('poly_two')
    message = 'Polygon data collection stopped with sequence #: %d' % p.sequence_number
    mail.send('POLY DONE', message)
    return 0
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--events','--event', '-e',help="poly event",type=str, default='T')
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    events = args.events.split(',') + ['AM']
    code = main(events)
    logging.info('done -- with exit code: %s' % code)
    exit(code)