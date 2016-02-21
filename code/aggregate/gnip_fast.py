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

import multiprocessing as mp




def getAllFilesRec(path, Format=None):
    '''returns all files in all folders inside specified of selected format'''

    for root, subdirs, files in os.walk(path):
        for filename in files:
            if Format == None or filename.endswith(Format):
                yield os.path.join(root, filename)
            else:
                pass


def parseTweet(tweet):
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
            tweet['generator']['displayName'])


def parseJsonArrayFile(path):
    dbPath = 'martin.db'
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()

    tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
		                                               timestamp INTEGER, 
		                                               lat REAL, lon REAL, 
		                                               tweet TEXT, 
		                                               user_id INTEGER, 
		                                               rtwts INTEGER,
		                                               fvrts INTEGER,
		                                               application TEXT)'''

    c.execute(tweetE)

    f = gzip.open(path,'rb')
    
    lines = f.read().split('\r')

    for line in lines[:-2]: # drop lase line
        if len(line)>2: # drop empty lines
            try:
                t = parseTweet(json.loads(line.strip()))
                c.execute(
	                    'INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', t)
                conn.commit()
            except Exception, e:
                print type(e), str(e)
                # print line
                pass
        else:
            pass

    f.close()
    conn.close()
    print '.'


def main(query):
    '''query meant to be path to files, for example: YYYY/MM/DD/HH/'''
    walk_dir = 'martin/' + query
    print walk_dir

    pool = mp.Pool(processes=12)


    
    file_paths = getAllFilesRec(walk_dir, Format='.json.gz')
    pool.map(parseJsonArrayFile, file_paths)



if __name__=='__main__':
    if len(sys.argv)<2:
        sys.stderr.write('USAGE: python %s <QUERY_PATH>\n' % sys.argv[0])
        sys.exit(1)

    main(sys.argv[1])
    print 'done! query =', sys.argv[1]