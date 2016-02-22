#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime
from dateutil import parser
import time
import sqlite3
import os
import ast


__author__ = "Philipp Kats"
__date__ = "2016_02_01"


def getFiles(folder, format, full=True, fltr=None):
    for file in os.listdir(folder):
        if file.endswith(format):
            if fltr==None or fltr not in file:
                if full:
                    yield os.path.join(folder, file)
                else:
                    yield file

def getSQLiteTweets(path):
    conn = sqlite3.connect(path)
    # timestamp INTEGER, lon REAL, lat REAL, tweet TEXT, user_id INTEGER, rtwts INTEGER, fvrts INTEGER, application TEXT, source TEXT
    df = conn.cursor().execute('SELECT id, timestamp, lon, lat, tweet, user_id, rtwts, fvrts, application, source FROM tweets').fetchall() ## all but raw data
    conn.close()
    return df



def parseTweets(path):
    t = (x for x in getSQLiteTweets(path) if x!=None)
    return t


def main(path):
    files = getFiles(path, '.db', full=True, fltr='aggregate')
    tweetE = 'CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, timestamp INTEGER, lon REAL, lat REAL, tweet TEXT, user_id INTEGER, rtwts INTEGER, fvrts INTEGER, application TEXT, source TEXT )'

    td = datetime.datetime.now().strftime('%Y-%m-%d %H|%M|%S')
    print td
    dbPath = path + '/%s-scraper_aggregate.db' % td


    conn = sqlite3.connect(dbPath)
    c = conn.cursor()
    c.execute(tweetE)

    for f in files:
        
        try:
            
            tweets = parseTweets(f)
            c.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets )
            print f.split('/')[-1], ' done!'
            
        except Exception, e:
            print f.split('/')[-1], str(e)

    conn.commit()
    conn.close()

    print 'Done'
    print dbPath

if __name__ == '__main__':
    path = os.getenv('PWD').replace('/util','/data')
    main(path)