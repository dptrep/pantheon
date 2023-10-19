'''
Created on Dec 2, 2021

@author: dan
'''
import logging
import requests
import json
import os

from src.core import plumbing
#from src.apps.operations.workers import luigi
from src.apps.data.core.workers import completed
from src.apps.data.polygon.models import poly_exchanges


POLY_TO_SIP = {'XASE':'A',
                'XBOS':'B',
                'XCIS':'C',
                'FINY':'DN',
                'FINN':'DB',
                'FINC':'DC',
                'XISE':'I',
                'EDGA':'J',
                'EDGX':'K',
                'XCHI':'M',
                'XNYS':'N',
                'ARCX':'P',
                'XNAS':'Q',
                'LTSE':'L',
                'IEXG':'V',
                'XPHL':'X',
                'BATY':'Y',
                'BATS':'Z',
                'EPRL':'H',
                'MEMX':'U'}


def get_data():
    url = '''https://api.polygon.io/v3/reference/exchanges?asset_class=stocks&locale=us&apiKey=hXWzmnDY7oPvNMdIyGpmGapeLBd8c9oY'''
    page = requests.get(url)
    j = json.loads(page.text)
    rpt = []
    for x in j['results']:
        x['poly_id'] = x['id']
        del x['id']
        if 'mic' in x:
            x['sip_code'] = POLY_TO_SIP.get(x['mic'], None)
            
        print(x)
        rpt += [x]
    return rpt

def get_max_id(table_class):
    sql = "select max(poly_id) from poly_exchanges"
    raw = table_class.query(sql, ['poly_id'])
    return raw[0]['poly_id']

def create_table():
    assert False, 'not implemented'
    pe = poly_exchanges.PolyExchanges()
    pe.create_table()
    pe.close_db()
    
def main():
    table_class = poly_exchanges.PolyExchanges()
    data = get_data()
    max_id = get_max_id(table_class)
    data = list(filter(lambda x: x['poly_id'] > max_id, data))
    n = len(data)
    if n > 0:
        table_class.post(data)
    
    full_file = os.path.abspath(__file__)
    completed.post(full_file=full_file, 
               request={'date':plumbing.today()}, 
               result= {'n':n},
               done=n>0) 

if __name__ == '__main__':
    main()