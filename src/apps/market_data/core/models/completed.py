'''
Luigi tracks the running of data collection processes

by: Dan Trepanier
date: May 27, 2023


'''

from src.core import model

class Completed(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'completed'
        self.columns    = [ 'id',
                            'date','time',
                            'host_name',
                            'path',
                            'process_name',
                            'request',
                            'result',
                            'done',
                           ]
        self.types      = ['SERIAL PRIMARY KEY',
                           'DATE', 'TIME',
                           'TEXT', 
                           'TEXT',
                           'TEXT', 
                           'JSON', 
                           'JSON', 
                           'BOOLEAN']
            
        model.Model.__init__(self)

if __name__ == '__main__':
    c = Completed()
    c.create_table()