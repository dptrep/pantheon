'''
Created on Mar 23, 2018

@author: dan

modified Jul 23, 2022
to run off polygon dividend query 
   "ex_dividend_date": "2021-11-05", -- date
   "cash_amount": 0.22,
   
   "dividend_type": "CD", -- what other values?
        CD - cash
        SC - special cash
        LT - 
        ST - 
   "declaration_date": "2021-10-28",
   "record_date": "2021-11-08",
   
   "pay_date": "2021-11-11",
   "frequency": 4,  -- quarterly?
   
   
   "ticker": "AAPL"

archived yahoo data to data/dividends/
'''

from src.core import model

class Dividends(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'dividends'
        self.columns    = ['id',
                           'posting_date','symbol',
                           'declaration_date','record_date',
                           'ex_dividend_date','pay_date',
                           'cash_amount',
                           'dividend_type',
                           'frequency',
                           ]
        self.types      = ['SERIAL PRIMARY KEY',
                           'DATE','TEXT',
                           'DATE','DATE',
                           'DATE','DATE',
                           'DOUBLE PRECISION',
                           'TEXT',
                           'INTEGER',
                           ]
        model.Model.__init__(self)

if __name__ == '__main__':
    pass