'''
Created on Nov 20, 2021

@author: dan
'''


from src.core import model

class PolyBar(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'poly_bar'
        self.columns    = ['id',
                           'date','time',
                           'symbol',
                           'open','high','low','close',
                           'average',
                           'volume',
                           'count',
                           ]
        self.types      = ['SERIAL PRIMARY KEY', 
                           'DATE', 'TIME', 
                            'TEXT',
                            'DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION',
                            'DOUBLE PRECISION',
                            'BIGINT',
                            'BIGINT',
                            ]
                           
        model.Model.__init__(self)

if __name__ == "__main__":
    #i = PolyBar()
    #i.create_table()
    pass