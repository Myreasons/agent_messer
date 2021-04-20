import datetime as dt
from Settings import path_to_datebase
import sqlite3
import pandas as pd
from nptime import nptime
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

last_worker_datetime = {}


def db_searcher(name):
    con = sqlite3.connect(path_to_datebase)
    today = dt.date.today()
    query = '''
            SELECT DISTINCT max(Datetime)
            From Events
            Where [Task name] = '%s' ''' % name + '''
            AND date(Datetime) = '%s' ''' % today

    df = (pd.read_sql(query, con))
    df['max(Datetime)'] = pd.to_datetime(df['max(Datetime)'])
    return (df['max(Datetime)'][0])


def date_validator(period, times, name, day_of_month=''):
    today = dt.date.today()

    try:
        last_done = last_worker_datetime[name]
    except:
        last_done = db_searcher(name)

    # dayly check
    if period == 'Every day':
        if (last_done is None) or ('nattype' in str(type(last_done))):
            i = 0
            for interval in times:
                if (dt.datetime.now().time() >= interval):
                    return (True)
                else:
                    i += 1
            if i > 0:
                return False
        else:
            try:
                '''последняя датавремя выполнения задачи < любой датывремени в списке
                и сейчас время >= первый минимальный элемент из этого списка'''
                last_done = nptime(last_done.hour, last_done.minute, last_done.second)
                for interval in times:
                    if (interval > last_done) and (dt.datetime.now().time() >= interval):
                        return (True)
                return (False)
            except:
                return (True)

    # weekly check
    elif today.strftime('%A') in period:
        if (last_done is None) or ('nattype' in str(type(last_done))):
            i = 0
            for interval in times:
                if (dt.datetime.now().time() >= interval):
                    return (True)
                else:
                    i += 1
            if i > 0:
                return False
        else:
            try:
                '''последняя датавремя выполнения задачи < любой датывремени в списке
                и сейчас время >= первый минимальный элемент из этого списка'''
                last_done = nptime(last_done.hour, last_done.minute, last_done.second)
                for interval in times:
                    if (interval > last_done) and (dt.datetime.now().time() >= interval):
                        return (True)
                return (False)
            except:
                return (True)

    # monthly check
    elif (str(today.day) in str(day_of_month)) and (period == 'Every month'):
        if (last_done is None) or ('nattype' in str(type(last_done))):
            i = 0
            for interval in times:
                if (dt.datetime.now().time() >= interval):
                    return (True)
                else:
                    i += 1
            if i > 0:
                return False
        else:
            try:
                '''последняя датавремя выполнения задачи < любой датывремени в списке
                и сейчас время >= первый минимальный элемент из этого списка'''
                last_done = nptime(last_done.hour, last_done.minute, last_done.second)
                for interval in times:
                    if (interval > last_done) and (dt.datetime.now().time() >= interval):
                        return (True)
                return (False)
            except:
                return (True)
