'''
Luigi tracks the running of data collection processes

by: Dan Trepanier
date: May 27, 2023

'''

import logging
import socket
import json

from src.core import plumbing
from src.apps.data.core.models import completed

def create_table():
    assert False, 'you may be trying to initialize completed by mistake!'
    c = completed.Completed()
    c.create_table()
    c.close_db()


def post(full_file: str, request: str, result: str, done: bool) -> None:
    '''
    Args:
        full_file    : '/Users/dan/Documents/workspace/pantheon/src/apps/core/workers/completed.py'
        request      : {'date':'20230301'}
        result       : 'success'
        done         : True
    '''
    today = plumbing.today()
    now = plumbing.current_time(time_unit='s', normal=True)
    
    h = socket.gethostname()
    host_name = h.split('.')[0]

    tt = full_file.split('/')
    path = '/'.join(tt[:-1]) + '/'

    ss = tt[-1].split('.')
    assert len(ss) == 2
    process_name = ss[0]
    if type(request) == dict:
        request = json.dumps(request)
    if type(result) == dict:
        result = json.dumps(result)

    r = {'date': today,
         'time': now,
         'host_name': host_name,
         'path': path,
         'process_name': process_name,
         'request': request,
         'result': result,
         'done': done}
    
    logging.info('luigi : %s' % r)
    
    c = completed.Completed()
    c.post([r])
    