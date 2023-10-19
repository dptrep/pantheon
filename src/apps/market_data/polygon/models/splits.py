'''
Created on Mar 23, 2018

@author: dan
'''


from src.core import model

class Splits(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'splits'
        self.columns    = ['id',
                           'date',
                           'symbol',
                           'before','after']
        self.types      = ['SERIAL PRIMARY KEY',
                           'DATE',
                           'TEXT',
                           'NUMBER','NUMBER']
        model.Model.__init__(self)

if __name__ == '__main__':
    pass