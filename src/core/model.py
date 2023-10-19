'''
Model Core Class

Created on Mar 23, 2018

@author:    Dan Trepanier
            Trep Capital, LLC
'''

import logging
import sqlite3
import psycopg2
import psycopg2.extras
import numpy as np
import math
import sys#,traceback
import datetime as dt
from src.apps import settings
#from . import plumbing

def get_date_string( day ):
    if type(day) in (dt.date, dt.timedelta):
        return day.strftime('%Y%m%d')
    else:
        return str(day)

class Model(object):
    def __init__(self, *args, **kwargs):
        assert len(self.columns) == len(self.types), 'columns (%d): %s\ntypes (%d): %s' % (len(self.columns), str(self.columns), len(self.types), str(self.types))
        self.file_name = None
        self.backend = self.get_backend()
    
    def get_backend(self):
        switch = {'INTEGER PRIMARY KEY': Sqlite, 'SERIAL PRIMARY KEY':Psql}
        return switch[self.types[0]](self.file_name, self.table_name, self.columns, self.types)
        
    def open_db(self):
        return self.backend.open_db()
        
    def close_db(self):
        return self.backend.close_db()
    
    def create_table(self, check=True):
        if check:
            sql_stmt = "SELECT tablename FROM pg_catalog.pg_tables;"
            available = list(map(lambda x: x['table_name'], self.query(sql_stmt, ['table_name'])))
            if self.table_name in available:
                logging.warning('table %s already exists' % self.table_name)
                sql_stmt = "DROP TABLE IF EXISTS %s;" % self.table_name
                logging.info(sql_stmt)
                choice = input('are you sure (Y/N)')
                if choice.lower() != 'y':
                    logging.info('skipping create table')
                    return False
                #else:
                    
            else:
                logging.warning('table %s appears to be new\nexisting tables: %s' % (self.table_name, sorted(available)))
            
        head = ', '.join(['%s %s' % (c_t[0],c_t[1]) for c_t in zip(self.columns, self.types)])
        self.db = self.open_db()
        c = self.db.cursor()
        sql_stmt = "DROP TABLE IF EXISTS %s;" % self.table_name
        c.execute(sql_stmt)
        logging.info('creating new table: %s' % self.table_name)
        #c.execute(sql_stmt)
        #print "CREATE TABLE %s (%s)" % (self.table_name, head)
        sql = "CREATE TABLE %s (%s)" % (self.table_name, head)
        logging.info(sql)
        c.execute(sql)
        self.db.commit()
        self.db = self.close_db()
        return True
    
    def post(self, data, on_conflict:str=None) -> int:
        '''
        input:
            data        : list of dict
            on_conflict : {'DO NOTHING'}
        '''
        self.db = self.open_db()
        cursor = self.db.cursor()
        for line in data:
            #print('GOT',line, 'conn',self.db,'cursor',cursor)
            sql_stmt = self.backend.insert(line, cursor, on_conflict)
            logging.debug(sql_stmt)
        r = self.db.commit()
        self.close_db()
        logging.info('posted %s: %d lines' % (self.table_name, len(data)))
        return len(data)
        
    def query(self, sql_stmt, columns=None, output='dict', string_dates=True, func=None):
        assert output in ('list','dict')
        return self.backend.query(sql_stmt, columns, output, string_dates, func)
    
    def run_sql(self, sql_stmt):
        self.db = self.open_db()
        cursor = self.db.cursor()
        tmp = cursor.execute(sql_stmt)
        r = self.db.commit()
        self.close_db()
        return tmp

    def get_iterator(self, sql_stmt, output='dict'):
        assert output in ('list','dict')
        return self.backend.get_iterator(sql_stmt, output)
        
    def get_cursor(self, sql_stmt, output='dict'):
        return self.backend.get_cursor(sql_stmt, output)
        
    def get_max_date(self, field='date'):
        sql_stmt = "select max(%s) from %s" % (field, self.table_name)
        tmp = self.query(sql_stmt, [field])
        if len(tmp) == 1:
            d = tmp[0][field]
            if d == 'None':
                return None
            else:
                return get_date_string(d)
        else:
            assert len(tmp) == 0
            return None
    
    def delete(self, sql_stmt, check=True):
        self.db = self.open_db()
        cursor = self.db.cursor()
        logging.info(sql_stmt)
        if check:
            choice = input('are you sure (Y/N)')
            if choice.lower() != 'y':
                logging.info('skipping delete')
                return False
        try:
            cursor.execute(sql_stmt)
        except:
            logging.warning('unable to execute: %s' % sql_stmt)
        self.db.commit()
        return True

class Psql(object):
    def __init__(self, file_name, table_name, columns, types):
        self.file_name = file_name
        self.table_name = table_name
        self.columns = columns
        self.types = types
        self.db = None
    
    def open_db(self):
        if self.db:
            return self.db
        cmd = "host=%s user=%s password=%s dbname=%s " % (settings.DB_HOST, settings.DB_USER, settings.DB_PASSWORD, settings.DB_NAME)
        try:
            self.db = psycopg2.connect(cmd)
            return self.db
        except:
            logging.warning('unable to connect to psql db : %' % cmd)
            sys.exit(1)
    
    def close_db(self):
        if self.db:
            self.db.close()
            self.db = None
            
    def insert(self, x, cursor, on_conflict):
        assert on_conflict in (None, 'DO NOTHING')
        logging.debug('input: %s' % str(x))
        assert self.columns[0] == 'id'
        percents = []
        values = []
        columns = []
        
        for c in self.columns[1:]:
            v = x.get(c, None)
            if c in x and v == v and v not in (None, 'None') :
                columns += [c]
                values += [v]
                percents += ['%s']
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table_name, ','.join(columns), ','.join(percents))
        if on_conflict is not None:
            sql += ' ON CONFLICT %s;' % on_conflict
        logging.debug('%s %s' % (sql,values))
        if cursor:
            #print('sql',sql)
            #print('values',values)
            #try:
            cursor.execute(sql, values)
            #except:
            #    print('had problems on insert here')
            #    print(sql)
            #    print(values)
            #    exit(1)
        return sql
    
    def query(self, sql_stmt, columns=None, output='dict', string_dates=True, func=None):
        self.open_db()
        cursor = self.db.cursor()
        if columns is None and output == 'dict':
            cursor = self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        #try:
        
        tmp = cursor.execute(sql_stmt)
        result = cursor.fetchall()
        #except:
        #    logging.warning('bad query: %s' % sql_stmt)
        #    exit(1)
        self.close_db()
        
        if columns is None:        
            return result
        else:
            assert output == 'dict'
            functions = {}
            if string_dates:
                for (c,t) in zip(self.columns, self.types):
                    if t == 'DATE':
                        functions[c] = get_date_string
            
            answer = []
            for x in result:
                row = dict(list(zip(columns, x)))
                for (c,f) in functions.items():
                    if c in row:
                        row[c] = f(row[c])
                answer += [row]
            #return [dict(list(zip(columns, x))) for x in result]
            return answer
    
    def get_iterator(self, sql_stmt, output='dict'):
        assert output in ('list','dict')
        self.open_db()
        cursor = self.db.cursor()
        if output == 'dict':
            cursor = self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql_stmt)
        return cursor
    
    def get_cursor(self, sql_stmt, output='dict'):
        assert output in ('list','dict')
        self.open_db()
        cursor = self.db.cursor()
        if output == 'dict':
            #cursor = self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor = self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql_stmt)
        return cursor
    
class Sqlite(object):
    def __init__(self, file_name, table_name, columns, types):
        self.file_name = file_name
        self.table_name = table_name
        self.columns = columns
        self.types = types
        self.screen_switch = {'INTEGER PRIMARY KEY': '%d',
                              'TEXT':"'%s'",
                              'INTEGER': '%d',
                              'NUMBER': '%.4f',
                              'DOUBLE PRECISION': '%.8f',
                              'BOOLEAN':'%s'}
        
        self.raw_switch     = {'INTEGER PRIMARY KEY': int,
                              'TEXT':str,
                              'INTEGER': int,
                              'NUMBER': float,
                              'DOUBLE PRECISION': float,
                              'BOOLEAN':bool}
        self.db         = None
        self.type_dict  = dict(list(zip(self.columns, self.types)))
        
    def get_screen_format(self, column, value=None):
        if value is None:   return self.screen_switch[self.type_dict[column]]
        else:               return self.screen_switch[self.type_dict[column]] % (value)
                   
    
    def get_raw_format(self, column, value=None):
        if value:       return self.raw_switch[self.type_dict[column]](value)
        else:           return self.raw_switch[self.type_dict[column]]
    
    def get_sql_format(self, column):
        return self.type_dict[column]
    
    def open_db(self):
        if self.db:
            return self.db
        try:
            self.db = sqlite3.connect(self.file_name, check_same_thread=False, timeout=3000)
            return self.db
        except sqlite3.Error as err:
            logging.warning('unable to open %s table %s' % (self.file_name, self.table_name))
            logging.warning(str(err))
            sys.exit(1)
    
    def close_db(self):
        if self.db:
            self.db.close()
            self.db = None
    
    def insert(self, line, cursor=None, on_conflict=None):
        good = []
        for (c,v) in list(line.items()):
            if c not in self.columns:
                pass
            elif v is None:
                pass
            elif self.get_sql_format(c) in ('NUMBER','DOUBLE PRECISION','INTEGER'):
                try:
                    value = float(v)
                    if math.isnan(v):
                        pass
                    else:
                        good += [c]
                except:
                    pass
            else:
                good += [c]
                 
        #good = filter(lambda c: c in self.columns and line[c] is not None and (self.get_sql_format(c) not in ('NUMBER','DOUBLE PRECISION','INTEGER') or not math.isnan(line[c])), line.keys())
        
        if len(good) == 0:
            logging.warning('no good columns found in data')
            print('data keys:',list(line.keys()))
            print('columns:',self.columns)
            print('good:',good)
            print('data',line)
            exit(1)
            
        else:
            try:
                values = [self.get_screen_format(c, line[c]) for c in good]
            except:
                logging.warning('unable to convert values')
                print(line)
                exit(1)
            sql_stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (self.table_name, ','.join(good), ','.join(values))
            if on_conflict is not None:
                sql_stmt += ' ON CONFLICT %s' % on_conflict
            #print sql_stmt
            if cursor:
                try:
                    cursor.execute( sql_stmt )
                except:
                    logging.warning('unable to insert data into %s' % self.table_name)
                    print('columns:',good)
                    print('values:',values)
                    print('types:',[self.get_sql_format(c) for c in good])
                    print('line',line)
                    print(sql_stmt)
                    exit(1)
            return sql_stmt
    
    def _set_output_format(self, output):
        assert output in ('dict','list')
        if output == 'dict':
            self.db.row_factory = sqlite3.Row
    
    def query(self, sql_stmt, columns=None, output='dict', func=None, *args, **kwargs):
        self.open_db()
        cursor = self.db.cursor()
        if columns is None:
            self._set_output_format(output)
            #try:
        tmp = cursor.execute(sql_stmt)
        result = tmp.fetchall()
        self.close_db()
        
        if columns is None:        
            return result
        else:
            assert output == 'dict'
            return [dict(list(zip(columns, x))) for x in result]
    
    def get_iterator(self, sql_stmt, output='dict'):
        assert output in ('list','dict')
        self.open_db()
        cursor = self.db.cursor()
        self._set_output_format(output)
        return cursor.execute(sql_stmt)
    
