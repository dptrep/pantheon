'''
Created on Nov 19, 2021

@author: dan

raw bars -- i.e. no split adjustment from polygon

'''

from src.core import model

class DayRaw(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'day_raw'
        self.columns    = ['id',
                           'date',
                           'symbol',
                           'open','high','low','close',
                           'vwap',
                           'volume',
                           'n']
        self.types      = ['SERIAL PRIMARY KEY',
                           'DATE', 
                           'TEXT',
                           'DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION',
                           'DOUBLE PRECISION',
                           'BIGINT',
                           'BIGINT']
        model.Model.__init__(self)

if __name__ == "__main__":
    pass