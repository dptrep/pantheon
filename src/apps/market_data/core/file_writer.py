'''
write redis messages to files
    - write a complete channel to each file
by: Dan Trepanier

May 12, 2022
'''


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(name)s] [%(levelname)s] : %(message)s')
#import argparse
import time
import redis
import os

from src import settings
#from src.apps.collector import db_writer
from src.core import plumbing, luigi_core

class FileWriter(object):
    def __init__(self, channel, event):
        self.channel = channel
        self.event = event
        self.date = plumbing.today()
        self.file = self.get_file()
    
    def get_file(self):
        location = settings.DIR + 'data/'
        plumbing.mk_dir_if_not_there(location, 'data_collection')
        full_location = location + 'data_collection/'
        file_name = '%s_%s_%s' % (self.channel, self.event, self.date)
        files = os.listdir(full_location)
        if file_name in files:
            mode = 'ab'
        else:
            mode = 'wb'
        logging.info('opening file: %s (mode=%s)' % (full_location + file_name, mode))
        f = open(full_location + file_name, mode)
        return f
    
    def write(self, msg):
        self.file.write(msg['data'] + b'\n')
    
    def close_file(self):
        self.file.close()


class Manager(object):
    def __init__(self) -> None:
        self.date = plumbing.today()
        self.redis = redis.Redis(**settings.REDIS_CONF_SIMS)
        self.pubsub = self.redis.pubsub()
        self.writers = self.get_file_writers()
        self.n = 0
        self.dt_write = 0
        self.last_time = time.time()
    
    def get_file_writers(self):
        writers = {}
        for event in ('T','Q','AM'):
            writers[event] = FileWriter(channel='SIP', event=event)
            channel = '%s.*.%s' % ('SIP', event)
            logging.info('subscribing to : %s' % channel)
            self.pubsub.psubscribe(**{channel:writers[event].write})
        for event in ('VWAP','BV','PUP',):
            writers[event] = FileWriter(channel='ALPHA', event=event)
            channel = '%s.*.%s*' % ('ALPHA', event)
            logging.info('subscribing to : %s' % channel)
            self.pubsub.psubscribe(**{channel:writers[event].write})
        return writers
    
    def start(self):
        self.thread = self.pubsub.run_in_thread(sleep_time=.001)

    def stop(self):
        self.thread.stop()
        for writer in self.writers.values():
            writer.close_file()


def main():
    today = plumbing.today()
    day_check = plumbing.is_trading_day(today)
    if not day_check:
        logging.warning('%s | today is not a trading day' % today)
        return 1
    f = Manager()
    f.start()
    logging.info('running')
    end_time = 3600 * 20
    now = plumbing.current_time(time_unit='s', time_zone='EST')
    while now < end_time:
        time.sleep(60)
        now = plumbing.current_time(time_unit='s', time_zone='EST')
        logging.info('FILE WRITER | now=%6d vs end_time=%6d' % (now, end_time))
    f.stop()
    luigi_core.done('file_writer')
    return 0

if __name__ == '__main__':
    logging.info('start')
    exit_code = main()
    logging.info('done')
    exit(exit_code)