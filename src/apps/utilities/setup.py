'''
general settings

Created on Mar 23, 2018

@author: dan trepanier
        Trep Capital
'''
import os

from src.apps import settings


from src.apps.data.core.workers import bar_avail
from src.apps.data.core.workers import completed

from src.apps.data.polygon.workers import day_raw
from src.apps.data.polygon.workers import dividends
from src.apps.data.polygon.workers import poly_bar
from src.apps.data.polygon.workers import splits

DIR = settings.DIR

def create_folders():
    local = ['data','logs']
    for sub in local:
        if not os.path.exists(DIR + sub):
            os.makedirs(DIR + sub)
    sub = ['incoming',
           'skew',
           'cboe_options',
           ]
    for sub in sub:
        if not os.path.exists(DIR + 'data/' + sub):
            os.makedirs(DIR + 'data/' + sub)

def create_tables():
    
    bar_avail.create_table()
    completed.create_table()
    
    day_raw.create_table()
    dividends.create_table()
    poly_bar.create_table()
    splits.create_table()
    


def main():
    create_folders()

if __name__ == '__main__':
    main()