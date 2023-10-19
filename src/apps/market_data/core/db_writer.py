'''
listens to redis and writes to psql

by: Dan Trepanier


'''

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
import argparse
import os
import time
import redis
import json
import psycopg2
import sys
from multiprocessing import Process

from src import settings
from src.core import plumbing
from src.core import luigi_core
from src.apps.platform.models import poly_am,poly_quotes_two,poly_trades_two
from src.apps.radio.models import vwap, value, pup

SWITCH = {'T':'SIP',
              'Q':'SIP',
              'AM':'SIP',
              'VWAP': 'ALPHA',
              'BV':'ALPHA',
              'PUP':'ALPHA'}

def insert_sip(x, event, date):
    sql = '''INSERT INTO poly_%s_%s (sequence_number, 
                                    posting_time, posting_micro,
                                    symbol, 
                                    msg)

            VALUES(%s, 
                    '%s', %s, 
                    '%s',
                    '%s')
            ''' % (event.lower(), 
                    date, 
                    x['sequence_number'],
                    x['posting_time'],
                    x['posting_micro'],
                    x['symbol'],
                    str(x['msg']))
    return sql

''
def insert_am(x, event, date):
    #print(x)
    all_columns    = [ 'date',
                    'sequence_number',
                    'posting_time','posting_micro',
                    'start_time',
                    'end_time',
                    'symbol',
                    'day_open','day_vwap','day_volume',
                    'open','high','low','close',
                    'volume',
                    'vwap','avg_size',
                    'msg_count',
                    ]
    all_types = ['DATE',
                    'BIGINT',
                    'TIME','BIGINT',
                    'TIME',
                    'TIME',
                    'TEXT',
                    'DOUBLE PRECISION','DOUBLE PRECISION','BIGINT',
                    'DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION',
                    'BIGINT',
                    'DOUBLE PRECISION','DOUBLE PRECISION',
                    'BIGINT']
    percents = []
    values = []
    columns = []
    
    for (c,t) in zip(all_columns, all_types):
        #print(c, x[c], t)
        if c in x and x.get(c,None) not in (None,'None'):
            v = x[c]
            columns += [c]
            values += [v]
            if t in ('DATE','TIME','TEXT'):
                percents += ["'%s'"]
            else:
                percents += ['%s']
    sql = "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING" % ('poly_am_%s' % date, ','.join(columns), ','.join(percents))
    #print(sql)
    final = sql % tuple(values)
    #print(final)
    return final


def insert_vwap(x, event, date):
    sql = '''INSERT INTO vwap_%s (date,
                                    posting_time, posting_micro,
                                    time, micro,
                                    sequence_number, 
                                    symbol, 
                                    event,
                                    value)

            VALUES('%s', 
                    '%s', %s, 
                    '%s', %s, 
                    %s,
                    '%s',
                    '%s',
                    %s)
            ''' % ( date,date,
                    x['posting_time'],x['posting_micro'],
                    x['time'], x['micro'],
                    x['sequence_number'],
                    x['symbol'],
                    x['event'],
                    x['value']
                    )
    return sql

def insert_value(x, event, date):
    sql = '''INSERT INTO value_%s (date,
                                    posting_time, posting_micro,
                                    time, micro,
                                    sequence_number, 
                                    symbol, 
                                    event,
                                    r,
                                    r_hat,
                                    value)

            VALUES('%s', 
                    '%s', %s, 
                    '%s', %s, 
                    %s,
                    '%s',
                    '%s',
                     %s,
                     %s,
                     %s
                    )
            ''' % ( date, date,
                    x['posting_time'],x['posting_micro'],
                    x['time'], x['micro'],
                    x['sequence_number'],
                    x['symbol'],
                    x['event'],
                    x['r'],
                    x['r_hat'],
                    x['value']
                    )
    return sql

def insert_pup(x, event, date):
    sql = '''INSERT INTO pup_%s (date,
                                    time, micro,
                                    sequence_number, 
                                    symbol, 
                                    event,
                                    probability)

            VALUES('%s', 
                    '%s', %s, 
                    %s,
                    '%s',
                    '%s',
                     %s)
            ''' % ( date, date,
                    x['time'], x['micro'],
                    x['sequence_number'],
                    x['symbol'],
                    x['event'],
                    x['probability'],
                    )
    return sql

class DBWriter(object):
    def __init__(self, event: str, date: str) -> None:
        self.event = event
        self.date = date
        
        self.db = self.open_db()
        self.cursor = self.db.cursor()
        self.switch = {'T': insert_sip,
                       'Q': insert_sip,
                       'AM': insert_am,
                       'VWAP': insert_vwap,
                       'BV': insert_value,
                       'PUP': insert_pup}
        self.func = self.switch[event]
        self.n = 0
        
        self.last_time = time.time()
        self.dt_commit = 0
        self.dt_work = 0
            
    def post(self, x):
        start = time.time()
        sql = self.func(x, self.event, self.date)
        self.cursor.execute(sql)
        self.n += 1
        end = time.time()
        self.dt_work += (end - start) * 1e6
        dt = end - self.last_time
        if dt > 5 and self.n >= 100:
            rate = self.dt_work / self.n
            start_time = time.time()
            self.db.commit()
            end_time = time.time()
            self.dt_commit = (end_time - start_time) * 1e6
            logging.info('writing %s to db: %d -- avg : %d us per msg | work=%d us | commit=%d us' % (self.event, self.n, rate, self.dt_work, self.dt_commit))
            self.last_time = end
            self.n = 0
            self.dt_work = 0

    def open_db(self):
        cmd = "host=%s user=%s password=%s dbname=%s " % (settings.DB_HOST, settings.DB_USER, settings.DB_PASSWORD, settings.DB_NAME)
        try:
            db = psycopg2.connect(cmd)
            return db
        except:
            logging.warning('unable to connect to psql db : %' % cmd)
            sys.exit(1)
    
    def get_table(self):
        switch = {'T': poly_trades_two.PolyTradesTwo,
                  'Q': poly_quotes_two.PolyQuotesTwo,
                  'AM': poly_am.PolyAM,
                  'VWAP': vwap.VWAP,
                  'BV': value.Value,
                  'PUP': pup.Pup}
        return switch[self.event](self.date)
        
    def close_db(self):
        self.db.commit()
        if self.db:
            self.db.close()
            self.db = None
        

class Manager(object):
    def __init__(self, channel:str, event:str) -> None:
        assert channel in ('SIP','ALPHA')
        assert event in ('T','Q','AM','VWAP','BV','PUP')
        self.channel = channel
        self.event = event

        self.today = plumbing.today()
    
        self.redis = redis.Redis(**settings.REDIS_CONF_SIMS)
        self.pubsub = self.redis.pubsub()
        self.writer = DBWriter(event, date=self.today)
        self.subscribe()

        self.n = 0
        self.dt_write = 0
        self.last_time = time.time()
    
    def subscribe(self):
        channel = '%s.*.%s' % (self.channel, self.event)
        if self.channel == 'ALPHA':
            channel += '*'
        logging.info('subscribing to : %s' % channel)
        self.pubsub.psubscribe(**{channel:self.handler})
    
    def handler(self, msg):
        channel = msg['channel'].decode('ascii')
        if msg['type'] == 'pmessage':
            event = channel[-1]
            assert event == self.event[-1],'event %s does not match expectation %s' % (event, self.event)
            x = json.loads(msg['data'])
            self.writer.post(x)

    def start(self):
        self.thread = self.pubsub.run_in_thread(sleep_time=.001)

    def stop(self):
        self.thread.stop()
        self.writer.close_db()
        

def upload_to_db(channel, event, date):
    assert event in ('T','Q','AM','VWAP','BV'), 'event not supported -- PUP is usually written to db in real time'
    #assert event != 'AM'
    location = settings.DIR + 'data/'
    full_location = location + 'data_collection/'
    file_name = '%s_%s_%s' % (channel, event, date)
    files = os.listdir(full_location)
    if file_name in files:
        count = 0
        db = DBWriter(event=event, date=date)
        table = db.get_table()
        sql = '''select count(*) from %s''' % table.table_name
        raw = table.query(sql, ['count'])
        if raw[0]['count'] > 0:
            logging.warning('data already exists in %s: %d items' % (table.table_name, raw[0]['count']))
            exit(0)

        with open(full_location + file_name, 'rb') as f:
            while True:
                line = f.readline()
                if not line:
                    break
                x = json.loads(line)
                db.post(x)
                count += 1
        db.close_db()
        
        sql = '''select count(*) from %s''' % (table.table_name)
        raw = table.query(sql, ['count'])
        db_count = raw[0]['count']
        print('file records: %d' % count)
        print('db records  : %d' % db_count)
        if db_count == count:
            logging.info('database upload complete -- removing old file: %s' % file_name)
            os.remove(full_location + file_name)
        else:
            logging.info('database upload is not complete -- leaving file: %s' % file_name)

def collect(channel, event):
    m = Manager(channel, event)
    m.start()
    logging.info('running')
    end_time = 3600 * 20
    now = plumbing.current_time(time_unit='s', time_zone='EST')
    while now < end_time:
        time.sleep(60)
        now = plumbing.current_time(time_unit='s', time_zone='EST')
        logging.info('DB WRITER | now=%6d vs end_time=%6d' % (now, end_time))
    m.stop()
    luigi_core.done('db_writer_%s_%s' % (channel, event))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--event', '-e',help="poly event",type=str, choices=SWITCH.keys(),default='T')
    parser.add_argument('--date', '-d',help="upload date",type=str, default=None)
    args = parser.parse_args()
    logging.info('start')
    logging.info(args)
    channel = SWITCH[args.event]
    t = plumbing.current_time(time_unit='s', time_zone='EST')    
    if args.date is None:
        today = plumbing.today()
        trading_day = plumbing.is_trading_day(today)
        if not trading_day:
            logging.warning('date=%s | not trading day ' % today)
            exit(1)
        closing_time = plumbing.closing_time(today)
        if t < closing_time:
            collect(channel, args.event)
        else:
            date = plumbing.today()
            upload_to_db(channel, args.event, date)
    else:
        upload_to_db(channel, args.event, args.date)
    logging.info('done')
    exit(0)

