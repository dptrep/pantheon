'''
intraday bar data availability

by: Dan Trepanier

Sep 10, 2022

ib_trades = 30 second bars
poly_bar  = 60 second bars

'''

from src.core import model

class BarAvail(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'bar_avail'
        self.columns    = ['id',
                           'posting_date','posting_time',
                           'date','symbol','con_id',
                           'ib_trades',
                           'ib_vol',
                           'poly_bar',
                           'poly_am',
                           ]
        self.types      = ['SERIAL PRIMARY KEY', 
                          'DATE','TIME',
                           'DATE','TEXT','BIGINT',
                            'BIGINT',
                            'BIGINT',
                            'BIGINT',
                            'BIGINT',
                            ]
                           
        model.Model.__init__(self)

if __name__ == "__main__":
    #ba = BarAvail()
    #ba.create_table()
    pass