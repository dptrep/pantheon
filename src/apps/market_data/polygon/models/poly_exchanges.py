'''
Created on Dec 2, 2021

@author: dan
'''


from src.core import model

class PolyExchanges(model.Model):
    def __init__(self, *args, **kwargs):
        self.table_name = 'poly_exchanges'
        self.columns    = ['id',
                           'poly_id',
                           'type','exchange',
                           'asset_class',
                           'locale',
                           'name','acronym',
                           'mic','operating_mic',
                           'participant_id',
                           'sip_code',
                           'url'
                           ]
        self.types      = ['SERIAL PRIMARY KEY',
                           'INTEGER',
                           'TEXT','TEXT',
                           'TEXT', 
                           'TEXT',
                           'TEXT','TEXT',
                           'TEXT','TEXT',
                           'TEXT',
                           'TEXT',
                           'TEXT'
                            ]
                           
        model.Model.__init__(self)

if __name__ == "__main__":
    p = PolyExchanges()
    #p.create_table()
    