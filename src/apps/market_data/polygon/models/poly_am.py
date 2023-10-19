'''
Polygon snapshots

Created on: Apr 3, 2022

by: Dan Trepanier
'''
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MAIN - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(message)s')

from src.core import model
from src.apps.data.polygon.models import day_bar

class PolyAM(model.Model):
    def __init__(self, date, *args, **kwargs):
        assert len(date) == 8,'bad date %s' % date
        self.date = date
        self.table_name = 'poly_am_%s' % date
        self.columns    = [ 'id',
                            'date',
                            'sequence_number',
                            'posting_time','posting_micro',
                            'start_time',
                            'end_time',
                            'symbol',
                            'day_open','day_vwap','day_volume',
                            'open','high','low','close',
                            'volume',
                            'vwap','avg_size',
                            'msg_count',
                           ]
        
        self.types      = ['SERIAL PRIMARY KEY',
                           'DATE',
                           'BIGINT',
                           'TIME','BIGINT',
                           'TIME',
                           'TIME',
                           'TEXT',
                           'DOUBLE PRECISION','DOUBLE PRECISION','BIGINT',
                           'DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION','DOUBLE PRECISION',
                           'BIGINT',
                           'DOUBLE PRECISION','DOUBLE PRECISION',
                           'BIGINT'
                           ]
        model.Model.__init__(self)
        self.check_availability()

    def check_availability(self):
        days = get_available_days()
        if self.date not in days:
            self.create_table(check=False)
            sql = '''ALTER TABLE %s ALTER COLUMN symbol SET NOT NULL;''' % self.table_name
            logging.info(sql)
            self.run_sql(sql)
            sql = '''ALTER TABLE %s ALTER COLUMN end_time SET NOT NULL;''' % self.table_name
            logging.info(sql)
            self.run_sql(sql)
            sql = '''ALTER TABLE %s ADD CONSTRAINT one_bar_per_minute_%s UNIQUE(end_time,symbol);''' % (self.table_name, self.date)
            logging.info(sql)
            self.run_sql(sql)


def get_available_days():
    db = day_bar.DayBar()
    sql_stmt = "SELECT tablename FROM pg_catalog.pg_tables;" # columns = ['schema_name','table_name','table_owner', 'table_space', 'has_indexes','has_rules','has_triggers','row_security']
    raw = map(lambda x: x['table_name'], db.query(sql_stmt, ['table_name']))
    sub = filter(lambda x: 'poly_am_' in x, raw)
    return list(map(lambda x: x.split('_')[-1], sub))

if __name__ == '__main__':
    pass
    #pa = PolyAM('20230604')
    #pa.create_table()