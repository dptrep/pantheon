'''
miscellaneous useful functions

Created on Mar 23, 2018

@author: dan trepanier
        Trep Capital
        
'''

import logging
import datetime as dt
import pytz
import time
import calendar
import pandas as pd
import numpy as np
import os, sys, subprocess#csv, 
from prettytable import PrettyTable
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay
from functools import reduce
#from .. import settings

MISSING_DAYS = ['20181205', # George H.W. Bush funeral
                '20121029','20121030', # Hurricane Sandy
                '20070102' # Gerald Ford Funeral
                ]

HALF_DAYS = ['20180703','20190703',
             '20181123','20191129','20201127',
             '20181224','20191224',
             '20201127','20201224',
             '20211126',
             '20221125',
             '20230703','20231124',
             '20240703','20241129',
             '20241224','20251224',
             ]

class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

HOLIDAYS = {}

inst = USTradingCalendar()
for year in range(1990, 2030):
    if year == 2022:
        inst.rules += [Holiday('Juneteenth', month=6, day=19, observance=nearest_workday),]
    if year == 2021:
        # Dec 31, 2021 market is open!!!
        HOLIDAYS[year] = inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 30))
    else:
        HOLIDAYS[year] = inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))
    #if year >= 2022:
    #    HOLIDAYS[year] = inst.holidays()
    #    print(HOLIDAYS[year], type(HOLIDAYS[year]))
    
def comma_thousand(value, decimal=2):
    mult = 10 ** decimal
    v = float(int(value * mult + .5)) / float(mult)
    return format(v, ',')


def str_is_int(txt):
    try: 
        int(txt)
        return True
    except ValueError:
        return False

def get_holidays(year, format='date'):
    assert format in ('date','string')
    if year not in HOLIDAYS:
        try:
            HOLIDAYS[year] = inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))
        except:
            HOLIDAYS[year] = [dt.datetime(year, 1, 1),dt.datetime(year, 7, 4), dt.datetime(year, 12, 25)]
    days = HOLIDAYS[year]
    #inst = USTradingCalendar()
    #days = inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))
    if format == 'date':        return days
    else:                       return [d.strftime('%Y%m%d') for d in days] 

def get_all_days(start, end):
    if type(start) == str:      format = 'string'
    else:                       format = 'date'
    start = get_date_format(start)
    end = get_date_format(end)
    assert type(start) == type(end),'mismatched types between start (%s) and end (%s)' % (type(start), type(end))
    answer = []
    delta = end - start
    for i in range(delta.days + 1):
        d = (start + dt.timedelta(days=i))
        if format == 'string':  answer += [d.strftime('%Y%m%d')]
        else:                   answer += [d]
    return answer

def get_year( d ):
    if type(d) == str:      return d[:4]
    else:                   return d.year

def get_date_format( day ):
    try:
        return dt.date(int(day[:4]), int(day[4:6]), int(day[6:]))
    except:
        return day
    #if type(day) in (str,str):    return dt.date(int(day[:4]), int(day[4:6]), int(day[6:]))
    #else:                   return day

def get_date_string( day ):
    if type(day) in (dt.date, dt.timedelta):
        return day.strftime('%Y%m%d')
    else:
        return str(day)
    #else:                   return day.strftime('%Y%m%d')

def get_trading_days(start, end, format=None, include_half_days=True):
    if sys.version_info[0] < 3:
        logging.warning('this function gets tripped up by unicode in python 2.x')
    if format is None:
        if type(start) ==  str:     format = 'string'
        else:                       format = 'date'
    assert type(start) == type(end),'mismatched types between start (%s) and end (%s)' % (type(start), type(end))
    if start == end:
        return [start]
    elif start > end:
        logging.warning('this logic may not work everywhere')
        return []
    days = get_all_days(start, end)
    years = uniques([get_year(d) for d in days])
    holidays = reduce(lambda a,b: a+b, [list(get_holidays(int(y), format='string')) for y in years])
    holidays += MISSING_DAYS
    answer = []
    for d in days:
        day_str = get_date_string(d)
        day_date = get_date_format(d)
        half_day_test = include_half_days or day_str not in HALF_DAYS
        holiday_test = day_str not in holidays and is_a_weekday(day_str)
        if holiday_test and half_day_test:
            if format == 'string':  answer += [day_str]
            else:                   answer += [day_date]
    return answer

def today(format='string'):
    today = dt.date.today()
    if format == 'string':  return today.strftime('%Y%m%d')
    else:                   return today

def is_a_weekday(day):
    return get_day_of_week(day) not in (5,6)
    
def hyphen_date(d):
    year = int(d[0:4])
    month = int(d[4:6])
    day = int(d[6:])
    return '%04d-%02d-%02d' % (year, month, day)

def get_day_of_week(d):
    year = int(d[0:4])
    month = int(d[4:6])
    day = int(d[6:])
    return calendar.weekday(year,month,day)

def is_a_holiday(day):
    year = get_year(day)
    holidays = get_holidays(int(year), format='string')
    return day in holidays

def is_trading_day(day=None):
    if day is None:
        day = today()
    return is_a_weekday(day) and not is_a_holiday(day)

def is_market_open():
    day_check = is_trading_day(today())
    t = current_time('s') + 3600 * 3
    close_time = closing_time(today())
    return day_check and t > 34200 and t < close_time

def wait_til_open():
    countdown = 3600 * 24
    last = countdown
    
    switch = {0: 1,
              1: 10,
              2: 50,
              3: 250,
              4: 600
              }
    
    while countdown > 0:
        time = current_time(time_unit='s', time_zone='EST')
        countdown = max(int(34200 - time),0)
        if countdown == 0:
            return
        x = int(np.log10(countdown))
        step = switch[x]
        if last - countdown >= step:
            logging.info('Time remaining: %8d seconds -- next step: %d' % (countdown, step))
            last = countdown
    return

def get_last_trading_day(day, include_half_days=True):
    while True:
        day = get_yesterday(day)
        if is_trading_day(day):   
            return day

def get_most_recent_day(cutoff=57600):
    day = today()
    if current_time('s') < cutoff - 3600 * 3:
        day = get_yesterday(day)
    while not is_trading_day(day):
        day = get_yesterday(day)
    return day

def get_next_trading_day(day):
    while True:
        day = get_tomorrow(day)
        if is_trading_day(day):   
            return day

def get_yesterday(day):
    d = get_date_format(day)
    return get_date_string(d - dt.timedelta(days=1))

def get_tomorrow(day):
    d = get_date_format(day)
    return get_date_string(d + dt.timedelta(days=1))

def get_last_trading_days(day, count, include_half_days=True, format='string'):
    assert format in (None, 'string','date')
    #logging.debug('no longer includes current day in the answer %s: %s' % (str(day), str(count)))
    if count == 0:
        return []
    else:
        yesterday = get_last_trading_day(day)
        end = get_date_format(yesterday)
        start = get_date_string(end - dt.timedelta(days=count*2))
        days = get_trading_days(start, yesterday, format=format, include_half_days=include_half_days)
        return days[-count:]

def get_next_trading_days(day, count):
    answer = []
    while len(answer) < count:
        day = get_next_trading_day(day)
        answer += [day]
    return answer

def get_last_day_of_month(yyyy, mm):
    if int(mm) == 12:
        first_of_next_month = dt.date(int(yyyy) + 1, 0o1, 0o1)
    else:
        first_of_next_month = dt.date(int(yyyy), int(mm) + 1, 0o1)
    return get_date_string(first_of_next_month - dt.timedelta(days=1))

#from datetime import datetime 

def get_month_ends(start, end):
    days = get_trading_days(start, end)
    month_ends = []
    last = None
    for day in days:
        if last is not None:
            mm_0 = last[4:6]
            mm_1 = day[4:6]
            if mm_0 != mm_1:
                month_ends += [last]
        last = day
    return month_ends

def get_third_friday(yyyy, mm):
    for dd in range(15, 22):
        d = dt.datetime(int(yyyy), int(mm), dd)
        if d.weekday() == 4:
            return d.strftime('%Y%m%d')
    assert False, 'could not find third Friday for %s %s' % (yyyy,mm) 

def time_delta(sooner, later):
    days = []
    for day in [sooner, later]:
        yyyy = int(day[:4])
        mm = int(day[4:6])
        dd = int(day[6:])
        days += [dt.date(yyyy,mm,dd)]
    delta = days[1] - days[0]
    return delta.days

def datetime_time_delta(sooner:dt.datetime, 
                        later:dt.datetime) -> dt.timedelta:
    d = dt.date.today()

    t_1 = dt.datetime(d.year, d.month, d.day, sooner.hour, sooner.minute)
    t_2 = dt.datetime(d.year, d.month, d.day, later.hour, later.minute)
    delta = t_2 - t_1
    return delta


def get_expiration_date(date, lookahead ):
    days = get_next_trading_days(date, count=lookahead)
    days.reverse()
    for d in days:
        d_dt = dt.datetime(int(d[:4]), int(d[4:6]), int(d[6:]))
        if d_dt.weekday() == 4:
            return d
    return max(days)


def split_days(days:list, bins:list=[.5,.5]) -> list:
        '''
        utility function that allocates the available days proportionately
         - the bin can contain percentages that add up to 1 (or 100)
         - the bin can also contain ratios
        
        this fucnction will allocate days proportionately between bins

        input:
            bins    : this is a list of percentages or integers to form proportions
            days    : list of days: ['yyyymmdd', ...]
        output:
            list of lists (the number of lists == number of bins)):
                [ [d_1, d_2, ...],
                  [d_n, d_n+1, ...],
                  [d_m, d_m+1, ...],
                ]

        '''
        days = sorted(days)
        total = sum(bins)
        lst = np.array(bins) / total
        n = len(days)
        start = 0
        results = []
        cum = 0
        bin_count = len(lst)
        for i,pcent in enumerate(lst):
            cum += pcent
            end = int(n*cum)
            if i == bin_count - 1:
                dd = days[start:]
            else:
                dd = days[start:end]
            logging.info('BIN %d : %s to %s' % (i+1, min(dd), max(dd)))
            results += [dd]
            start = end
        return results

'''
time related
'''

def datetime_time_delta(sooner:dt.time, later:dt.time, date:dt.date=None):
    if date is None:
        date = dt.date.today()
    dateTimeA = dt.datetime.combine(date, sooner)
    dateTimeB = dt.datetime.combine(date, later)
    # Get the difference between datetimes (as timedelta)
    return dateTimeB - dateTimeA

def adjust_time_by_seconds(date:str, time:dt.time, delta:int):
    t_dt = dt.datetime(int(date[:4]), int(date[4:6]), int(date[6:]),time.hour, time.minute, time.second)
    new = t_dt + dt.timedelta(seconds=delta)
    return dt.time(new.hour, new.minute, new.second)

def one_second(time_unit):
    if time_unit == 'ns':               return 1e9
    elif time_unit == 'us':             return 1e6
    elif time_unit == 'ms':             return 1e3
    elif time_unit == 's':              return 1
    print('bad time unit', time_unit)

def closing_time(date, time_unit='s', normal=False):
    mult = one_second(time_unit)
    if date in HALF_DAYS:
        if normal:
            return '13:00:00'
        else:
            return 13 * 3600 * mult
    else:
        if normal:
            return '16:00:00'
        else:                   
            return 57600

def current_time(time_unit = 'ns', normal=False, time_zone='PST'):
    switch = {'EST':pytz.timezone('US/Eastern'),
              'PST':pytz.timezone('US/Pacific')}
    tz = switch[time_zone]
    now = dt.datetime.now(tz)
    t = dt.datetime.time(now)
    time_in_seconds = t.hour * 3600 + t.minute * 60 + t.second + 1e-6 * t.microsecond
    t = time_in_seconds
    t *= one_second(time_unit)
    if normal:  return normal_time(t, time_unit=time_unit)
    else:       return t 

def datetime_to_abs_time( value, time_unit = 'ns'):
    t_ratio = one_second( time_unit ) / 1e6
    t = ((value.hour * 3600 + value.minute * 60 + value.second) * 1e6 + value.microsecond) * t_ratio
    if time_unit in ('us','ns'):    return int(t)
    else:                           return t

def abs_time(value, time_unit='ns'):
    if type(value) == dt.time:
        t = value.hour * 3600 + value.minute * 60 + value.second
    else:
        tmp = value.split(':')
        t = ( int(tmp[0]) * 3600 + int(tmp[1]) * 60 + int(tmp[2]) )
    t *= one_second( time_unit )# / 1e6
    if time_unit in ('us','ns'):    return int(t)
    else:                           return t


def get_time_format(t):
    try:
        tmp = t.split(':')
        return dt.time(int(tmp[0]), int(tmp[1]), int(tmp[2]))
    except:
        return t
    #if type(day) in (str,str):    return dt.date(int(day[:4]), int(day[4:6]), int(day[6:]))
    #else:                   return day
    
def normal_time(value:float, time_unit:str='ns', fmt:str='str'):
    '''
    input:
        value       : time from midnight
        time_unit   : {'ns','us', 'ms','s'}
        fmt         : return format {'str','time'}
    output:
        when fmt=='str' -> 'hh:mm:ss'
        when fmt=='time' -> datetime.time(h,m,s)
    '''
    assert fmt in ('str','time')
    if time_unit == 'ns':       value = int(value / 1e9)
    elif time_unit == 'us':     value = int(value / 1e6)
    elif time_unit == 'ms':     value = int(value / 1e3)
    else:                       assert time_unit == 's'
    t = time.strftime('%H:%M:%S', time.gmtime(value))
    if fmt == 'str':
        return t
    else:
        h,m,s = t.split(':')
        return dt.time(int(h), int(m), int(s))
        
def get_date_time(ts):
    '''
    date, time, and microseconds
    '''
    d = dt.datetime.fromtimestamp(ts / 1000.0) + dt.timedelta(hours=3)
    date = '%d%02d%02d' % (d.year, d.month, d.day)
    time = '%02d:%02d:%02d' % (d.hour, d.minute, d.second)
    return (date, time, d.microsecond)

def join_datetime(date, time, micro=None):
    if type(date) == str:
        y,M,d = int(date[:4]), int(date[4:6]), int(date[6:])
    else:
        y,M,d = date.year, date.month, date.day

    if type(time) == str:
        h,m,s = time.split(':')
        if micro is None:
            micro = 0
    elif time is None:
        return None
    else:
        h,m,s = time.hour, time.minute, time.second
        if micro is None:
            micro = time.microsecond
    return dt.datetime(y,M,d, int(h), int(m), int(s), int(micro))


'''
set related functions
'''
def uniques(values):
    return sorted(list(set(values)))

def set_to_list(values):
    return sorted(list(values))

'''
file related
'''

def mk_dir_if_not_there(root, dir_name):
    files = os.listdir(root)
    dir_name = dir_name.strip('/')
    if dir_name not in files:
        location = root + '/' + dir_name
        os.mkdir(location)
    

def read_csv(filepath_or_buffer, sep=',', output='dict', delimiter=None, *args, **kwargs):
    assert output in ('dict','pd','list')
    result = pd.read_csv(filepath_or_buffer, sep, *args, **kwargs)
    if output=='dict':          
        tmp = list(result.T.to_dict().values())
        answer = []
        for row in tmp:
            tmp = {}
            for (k,v) in list(row.items()):
                if 'date' in k:
                    if type(v) == str and '-' in v:
                        v = v.replace('-',"")
                    tmp[k] = str(int(v))
                else:
                    tmp[k] = v
            answer += [tmp]
        return answer
    elif output == 'list':      return [list(result.columns)] + result.values.tolist()
    elif output == 'pd':        return result

def write_csv( contents, prefix, revise=True, separator=',',extension='.csv'):
    if '/' in prefix:
        lst = prefix.split('/')
        working_dir = '/'.join(lst[:-1]) + '/'
        prefix = lst[-1]
    else:
        working_dir = os.getcwd() + '/'
    #print 'plumbing.Write -- working dir',working_dir
    #print 'file',prefix
    files = os.listdir(working_dir)
    header = contents[0]
    output = separator.join(header) + '\n'
    for line in contents[1:]:
        assert len(line) == len(header),'header (%d): %s\nline (%d): %s' % (len(header), str(header), len(line), str(line))
        output += separator.join([str(x) for x in line]) + '\n'
    if revise:
        rev = 0
        #ext = '.csv'
        file = prefix + str(rev) + extension 
        while file in files:
            rev += 1
            file = prefix + str(rev) + extension
    else:
        file = prefix + extension
    
    filename = working_dir + file 
    with open(filename, "w") as out:
        out.write(output)  
    return filename

'''
format related
'''

def screen_format( list_of_lists,**kwargs):
    t = PrettyTable(list_of_lists[0],**kwargs)
    for row in list_of_lists[1:]:
        t.add_row(row)
    return t


class FIFO(object):
    def __init__(self, depth):
        assert depth is None or depth >= 0
        self.depth = depth
        self.values = []
    
    def update(self, x):
        if self.depth == 0:
            return []
        self.values += [x]
        while self.depth is not None and len(self.values) > self.depth:
            self.values.pop(0)
        return self.values
'''
config related
'''
    
def get_git_revision_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()

'''
def get_current_git_revision():
    current_dir = os.getcwd()
    try:
        os.chdir(settings.GIT)# + 'colliseum/src/apps/platform/strategies/')
        p = subprocess.Popen("cat .git/refs/heads/master", stdout=subprocess.PIPE, shell=True)
        os.chdir(current_dir)
        (output, err) = p.communicate()
        if err:     logging.warning(err)
        else:       output = output.decode("utf-8").strip("\n'")
        return output
    except:
        logging.warning('fetching git from local directory')
        return get_git_revision_hash()
''' 

'''
math
'''
def safe_value(value, default=0):
    if value is None:   
        return default
    else:
        try:
            if np.isfinite(value):
                return value
            else:
                return default
        except:
            return default


def safe_list(lst):
    sub = []
    for x in lst:
        try:
            v = float(x)
        except:
            v = None
        if v is not None and np.isfinite(v):
            sub += [v]
    return sub

def safe_func(func, lst, default):
    try:
        vv = safe_list(lst)
        v = func(vv)
        return safe_value(v, default)
    except:
        return default

def safe_sum(values:list, default=0):
    vv = safe_list(values)
    if len(vv) == 0:
        return default
    else:
        return safe_func(sum, vv, default)
        

def safe_divide(num, denom, default=None):
    num = safe_value(num)
    denom = safe_value(denom)
    try:
        if denom == 0:
            return default
        else:
            return num / denom
    except:
        return default

def safe_multiply(first, second, default=None):
    first = safe_value(first)
    second = safe_value(second)
    try:
        return first * second
    except:
        return default


def safe_return(new_p, last_p):
    if last_p > 0:  return (new_p - last_p) / last_p
    else:           return 0

def safe_diff(b, a, default=None):
    a = safe_value(a)
    b = safe_value(b)
    if None in (b,a):
        return default
    else:
        try:
            return b - a
        except:
            return default

def safe_min(lst, default=None):
    sub = safe_list(lst)
    if len(sub) == 0:
        return default
    else:
        return min(sub)

def safe_max(lst, default=None):
    sub = safe_list(lst)
    if len(sub) == 0:
        return default
    else:
        return max(sub)

def safe_mean(lst, default=None):
    if len(lst) == 0:
        return default
    else:
        return safe_func(np.mean, lst, default)

'''
sql related
'''

def get_sql_list(lst:list):
    '''transform list into a sql string list'''
    if len(lst) == 0:
        return ''
    else:
        return '(%s)' % (','.join(["'%s'" % x for x in lst]))
        
    

if __name__ == '__main__':
    pass
    #print get_holidays(2017, 'string')
    #print get_all_days('20170101','20170131','string')
    #print get_trading_days('20161201','20170131')
    print(get_tomorrow('20170805'))
    print(get_next_trading_day('20170805'))
    #exit(1)
    
    print(get_last_trading_days('20111019',5))
    print(get_next_trading_days('20180202',10))
    #txt = read_csv('symbols.txt', output='list')
    #print txt
