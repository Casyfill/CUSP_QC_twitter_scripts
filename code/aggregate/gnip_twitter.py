#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime
import dateutil.parser
import time
import sqlite3
import gzip
import json
import os


__author__ = "Philipp Kats"
__date__ = "2016_02_01"


def getFiles(folder, format, full=True):
    for file in os.listdir(folder):
        if file.endswith(format):
            if full:
                yield os.path.join(folder, file)
            else:
                yield file

def parseJsonArrayFile(path):
    with gzip.open(path, 'rb') as f:
        lines = f.read().split('\r')
        records = []
        for line in lines:
            if len(line.strip())>0:
                try:
                    records.append( json.loads(line.strip()) )
                except Exception, e:
                    pass
        return records


def parseAll(paths):
    records = []
    for path in paths:
        records.extend(parseJsonArrayFile(path))
    
    return records

def parseTweet(tweet, mode='tpl'):
    if 'geo' in tweet:
        if mode=='tpl':
            return (int(tweet['id'].split(':')[-1]),
                    int(dateutil.parser.parse(tweet['postedTime']).strftime("%s")), #int(time.mktime(dateutil.parser.parse(tweet['postedTime']).timetuple())),
                    tweet['geo']['coordinates'][0],
                    tweet['geo']['coordinates'][1],
                    tweet['body'],
                    int(tweet['actor']['id'].split(':')[-1]),
                    tweet['retweetCount'],
                    tweet['favoritesCount'],
                    tweet['generator']['displayName'])
    else:
        pass

def toSQLite(folder, tweets):

	tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
	                                               timestamp INTEGER, 
	                                               lon REAL, lat REAL, 
	                                               tweet TEXT, 
	                                               user_id INTEGER, 
	                                               rtwts INTEGER,
	                                               fvrts INTEGER,
	                                               source TEXT)'''
	dbPath = folder + 'gnip_twitter.db'

	conn = sqlite3.connect(dbPath)
	c = conn.cursor()
	c.execute(tweetE)
	c.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets )
	conn.commit()
	conn.close()


def aggregate(folder):
    files = getFiles( folder, 
                      '.json.gz', 
                      full=True)

    records = parseAll(files)
    parcedRecords = [parseTweet(record) for record in records if parseTweet(record, mode='tpl')!=None]
    toSQLite(folder, parcedRecords)



if __name__ == '__main__':
	folder = sys.argv[1]
	if os.path.isdir(folder):
		aggregate(folder)
	else:
		print 'Wrong arguement'

	

	


