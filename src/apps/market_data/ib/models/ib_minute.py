'''
Created on Nov 20, 2021

@author: dan
'''

from src import settings
from src.core import model

class IbMinute(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'ib_minute'
        self.file_name  = settings.DB
        self.columns    = ['id',
                            'data_type',
                           'date','time',
                           'symbol','con_id',
                           'open','high','low','close',
                           'average',
                           'volume',
                           'count',
                           ]
        self.types      = ['SERIAL PRIMARY KEY', 
                            'TEXT',
                           'DATE', 'TIME', 
                            'TEXT','BIGINT',
                            'DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION',
                            'DOUBLE PRECISION',
                            'BIGINT',
                            'BIGINT',
                            ]
                           
        model.Model.__init__(self)

if __name__ == "__main__":
    i = IbMinute()
    i.create_table()
    