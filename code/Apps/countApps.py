#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
generates list of top N applications and counts of tweets per each
'''

from sqlalchemy import create_engine
from sys import argv
import os
import pandas as pd


def getFiles(folder, format, full=True, filtr=None):
    for f in os.listdir(folder):
        if f.endswith(format):
            if filtr is None or filtr not in f:
                if full:
                    yield os.path.join(folder, f)
                else:
                    yield f


def mostRecentFile(path, frmt='.db'):
    '''returns most recent .db file in folder'''

    def reduceRecent(current, newPath):
        d = os.stat(newPath).st_ctime
        if d > current[1]:
            return (newPath, d)

    path, time = reduce(reduceRecent, getFiles(path, frmt), (None, 0))
    return path


def drill(limit=None):
    fPath = mostRecentFile('DATAVAULT/OUT/', '.db')
    print fPath

    if limit:
        Q = 'SELECT application, count(application) as cnt FROM tweets GROUP by application ORDER BY cnt DESC LIMIT {0}'.format(limit)
    else:
        Q = 'SELECT application, count(application) as cnt FROM tweets GROUP by application ORDER BY cnt DESC'

    #Q2 = 'SELECT COUNT(id) FROM tweets'

    engine = create_engine('sqlite:///%s' % fPath)

    r = pd.read_sql_query(Q, con=engine)
    #t = pd.read_sql_query(Q2, con=engine).ix[0]
    #print t
    path = 'apps.csv'
    r.to_csv(path, index=False, encoding='utf8')

    # with open(path,'a') as fi:
    #     fi.write('Total,{0}'.format(t))
    print 'Saved to: {0}'.format(path)

if __name__ == '__main__':
    if len(argv) > 1:
        s = argv[1]
    else:
        s = None
    print 'Calculating {0} top apps'.format(s)
    drill(s)
