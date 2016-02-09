#!/usr/bin/env python
#-*- coding: utf-8 -*-


import sqlite3
import datetime
import dateutil.parser
import os
import ast

def getFiles(folder, format, full=True, filtr=None):
    for f in os.listdir(folder):
        if f.endswith(format):
            if filtr==None or filtr not in f:
                if full:
                    yield os.path.join(folder, f)
                else:
                    yield f

def getSQLiteTweets(path):
    conn = sqlite3.connect(path)
    df = conn.cursor().execute('SELECT id, timestamp, lon, lat, tweet, user_id, rtwts, fvrts, application, source FROM tweets').fetchall() ## all but raw data
    conn.close()
    return df



def mergeAll(path):
    dbPath = ''.join((path.replace('aggregated','out_DB') ,'/' , datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') , '_all.db'))
    
    tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
                                               timestamp INTEGER, 
                                               lon REAL, lat REAL, 
                                               tweet TEXT, 
                                               user_id INTEGER, 
                                               rtwts INTEGER,
                                               fvrts INTEGER,
                                               application TEXT,
                                               source TEXT )'''

    conn = sqlite3.connect(dbPath)
    c = conn.cursor()
    c.execute(tweetE)
    
    for f in getFiles(path, 'db', 'all'):
        print f
        if 'MERGED_ALL' not in f:
        	try:
	            tweets = getSQLiteTweets(f)
	            c.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets )
	            conn.commit()
	            print f.split('/')[-1], ' done!'
	        except Exception, e:
	        	print f.split('/')[-1], str(e)

    
    conn.close()
    print 'Databases Merged!'
    print dbPath

def main():
	path = os.getenv('PWD').replace('code/aggregate','data/aggregated')
	# print path
	mergeAll(path)

if __name__ == '__main__':
	main()