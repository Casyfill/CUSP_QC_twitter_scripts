#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os
import sys

import datetime
import dateutil.parser
import time

import sqlite3
import gzip
import json


walk_dir = sys.argv[1]

import os


def getAllFilesRec(path, Format=None):
    '''returns all files in all folders inside specified of selected format'''

    for root, subdirs, files in os.walk(walk_dir):
        for filename in files:
            if Format == None or filename.endswith(Format):
                yield os.path.join(root, filename)
            else:
                pass


def parseTweet(tweet, source):
    if 'geo' in tweet.keys():
        lat, lon = tweet['geo']['coordinates'][
            0], tweet['geo']['coordinates'][1]
    else:
        lat, lon = None, None

    return (int(tweet['id'].split(':')[-1]),
            # UTF TIME UNIX TIMESTAMP
            int(dateutil.parser.parse(tweet['postedTime']).strftime("%s")),
            lat,
            lon,
            tweet['body'],
            int(tweet['actor']['id'].split(':')[-1]),
    		int(tweet['retweetCount']),
            tweet.get('favoritesCount', None),
            tweet['generator']['displayName'],
            source)


def parseJsonArrayFile(path, dbPath):
    source = 'M'
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()

    tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
		                                               timestamp INTEGER, 
		                                               lat REAL, lon REAL, 
		                                               tweet TEXT, 
		                                               user_id INTEGER, 
		                                               rtwts INTEGER,
		                                               fvrts INTEGER,
		                                               application TEXT,
	                                                   source TEXT)'''

    c.execute(tweetE)

    with gzip.open(path, 'rb') as f:
        lines = f.read().split('\r')

        for line in lines[:-2]:
        	if len(line)>2:
	            try:
	                t = parseTweet(json.loads(line.strip()), source)
	                c.execute(
	                    'INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', t)
	                conn.commit()
	            except Exception, e:
	            	print type(e), str(e)
	            	print line
	            	pass
	        else:
	        	pass
    conn.close()


def main(walk_dir):
    dbPath = 'martin_db.db'

    for file_path in getAllFilesRec(walk_dir, Format='.json.gz'):
        try:
            parseJsonArrayFile(file_path, dbPath)
            print file_path
        except Exception, e:
            print str(e)


if __name__ == '__main__':
    main(sys.argv[1])
    print 'done!!!!!!!!'
