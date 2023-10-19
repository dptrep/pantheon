'''
polygon options master table

by: Dan Trepanier
date: Jan 3, 2022

https://polygon.io/docs/options/get_v3_reference_options_contracts


Correct way to create this table:
CREATE TABLE poly_opt_master 
        (id SERIAL PRIMARY KEY, 
        posting_date DATE, 
        cfi TEXT, 
        contract_type TEXT, 
        exercise_style TEXT, 
        expiration_date DATE, 
        primary_exchange TEXT, 
        shares_per_contract BIGINT, 
        strike_price DOUBLE PRECISION, 
        ticker TEXT, 
        underlying_ticker TEXT, 
        UNIQUE (ticker))
        ; 

ALTER TABLE poly_opt_master SET TABLESPACE archive;
'''
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(message)s')


from src.core import model

class PolyOptMaster(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'poly_opt_master'
        self.columns    = ['id',
                           'posting_date',
                           'cfi','contract_type',
                           'exercise_style',
                           'expiration_date',
                           
                           'primary_exchange',
                           
                           'shares_per_contract',
                           'strike_price',
                           'ticker',
                           'underlying_ticker'
                           ]
        self.types      = ['SERIAL PRIMARY KEY', 
                           'DATE', 
                            'TEXT','TEXT','TEXT',
                            'DATE', 
                            
                            'TEXT',
                            
                            'BIGINT',
                            'DOUBLE PRECISION',
                            'TEXT','TEXT',
                            ]
                           
        model.Model.__init__(self)

if __name__ == "__main__":
    pass