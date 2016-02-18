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

	result = []
	for root, subdirs, files in os.walk(walk_dir):
		for filename in files: 
			if Format==None or filename.endswith(Format):
				result.append(os.path.join(root, filename))
			else:
				pass

	return result

def parseTweet(tweet, source):
    return (int(tweet['id'].split(':')[-1]),
                    int(dateutil.parser.parse(tweet['postedTime']).strftime("%s")), # UTF TIME UNIX TIMESTAMP
                    tweet['geo']['coordinates'][0],
                    tweet['geo']['coordinates'][1],
                    tweet['body'],
                    int(tweet['actor']['id'].split(':')[-1]),
                    tweet['retweetCount'],
                    tweet['favoritesCount'],
                    tweet['generator']['displayName'],
                    source)


def parseJsonArrayFile(path, dbPath):
	source = path.split('/')[-2]   
	conn = sqlite3.connect(dbPath)
	c = conn.cursor()
	

	tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
		                                               timestamp INTEGER, 
		                                               lon REAL, lat REAL, 
		                                               tweet TEXT, 
		                                               user_id INTEGER, 
		                                               rtwts INTEGER,
		                                               fvrts INTEGER,
		                                               application TEXT,
	                                                   source TEXT)'''

	c.execute(tweetE)

	with gzip.open(path, 'rb') as f:
		lines = f.read().split('\r')

		for line in lines:
			if 'geo' in line:
				try:
					t = parseTweet(json.loads(line.strip()), source )
					c.execute('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', t )
					conn.commit()

				except Exception, e:
					pass
	conn.close()

def main(walk_dir, dbPath, ID):
	dbPath += '/%s.db' % ID
	try:
		for file_path in getAllFilesRec(walk_dir, Format='.json.gz'):
			parseJsonArrayFile(file_path, dbPath)
			print '.'
	except Exception, e:
		print str(e)


if __name__ == '__main__':
	main(sys.argv[1], sys.argv[2], sys.argv[3])