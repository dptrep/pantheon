'''
Created on Apr 22, 2022

@author: dan
'''
from src import settings
from src.apps.data.polygon.models import day_raw
from src.core import model

class PolyQuotes(model.Model):
    def __init__(self, date, *args, **kwargs):
        assert len(date) == 8,'bad date %s' % date
        assert date >= '20220422','for older data look at poly_quotes'
        self.table_name = 'poly_q_%s' % date
        self.date = date
        self.file_name  = settings.DB
        self.columns    = [ 'id',
                           'sequence_number',
                           'posting_time','posting_micro',
                           'symbol',
                           'msg'
                           ]
        
        self.types      = ['SERIAL PRIMARY KEY',
                           'BIGINT',
                           'TIME','BIGINT',
                           'TEXT',
                           'JSON'
                           ]        
        model.Model.__init__(self)
        self.check_availability()
        
    def check_availability(self):
        days = get_available_days()
        if self.date not in days:
            self.create_table(check=False)


def get_available_days():
    db = day_raw.DayRaw()
    sql_stmt = "SELECT tablename FROM pg_catalog.pg_tables;" # columns = ['schema_name','table_name','table_owner', 'table_space', 'has_indexes','has_rules','has_triggers','row_security']
    raw = map(lambda x: x['table_name'], db.query(sql_stmt, ['table_name']))
    sub = filter(lambda x: 'poly_q_' in x, raw)
    return list(map(lambda x: x.split('_')[-1], sub))
    
if __name__ == '__main__':
    p = PolyQuotes()
    #p.create_table()