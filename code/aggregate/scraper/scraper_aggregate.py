#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime
import dateutil.parser
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
    df = conn.cursor().execute('SELECT id, tweet, geo, timestamp, user_id FROM tweets').fetchall() ## all but raw data
    conn.close()
    return df

def parseRow(row, aggr_id='?'):
    '''parse row to the defined schema'''
    if row[2] != 'null':
        geo =  ast.literal_eval(row[2])['coordinates']
        return (int(row[0]),                                        # tweet id
                int(dateutil.parser.parse(row[3]).strftime("%s")),  # timestamp
                geo[0],                                             # lon (geo)
                geo[1],                                             # lat (geo)
                row[1],                                             # text body
                int(row[4]),                                        # user id
                None,
                None,
                None,
                aggr_id                                             # db id
                )
    else:
        pass


def parseTweets(path, ID='?'):
    t = [parseRow(x, ID) for x in getSQLiteTweets(path) if parseRow(x, ID)!=None]
    return t


def main(path):
    files = getFiles(path, '.db', full=True, fltr='aggregate')
    tweetE = 'CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, timestamp INTEGER, lon REAL, lat REAL, tweet TEXT, user_id INTEGER, rtwts INTEGER, fvrts INTEGER, application TEXT, source TEXT )'

    td = datetime.datetime.now().strftime('%Y-%m-%d %H|%M|%S')
    print td
    dbPath = path.replace('all_scraper','aggregated') + '/%s-scraper_aggregate.db' % td


    conn = sqlite3.connect(dbPath)
    c = conn.cursor()
    c.execute(tweetE)

    for f in files:
        
        try:
            
            tweets = parseTweets(f, ID=unicode(f.split('/')[-1]))
            c.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets )
            print f.split('/')[-1], ' done!'
            
        except Exception, e:
            print f.split('/')[-1], str(e)

    conn.commit()
    conn.close()

    print 'Done'
    print dbPath

if __name__ == '__main__':
    path = os.getenv('PWD').replace('code/aggregate','data/all_scraper')
    main(path)