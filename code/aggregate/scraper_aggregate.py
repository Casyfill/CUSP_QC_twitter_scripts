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


def getFiles(folder, format, full=True):
    for file in os.listdir(folder):
        if file.endswith(format):
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
    geo =  ast.literal_eval(row[2])['coordinates']
    return (int(row[0]),                                        # tweet id
            int(dateutil.parser.parse(row[3]).strftime("%s")),  # timestamp
            geo[0],                                             # lon (geo)
            geo[1],                                             # lat (geo)
            row[1],                                             # text body
            int(row[4]),                                        # user id
            'homebrew-%s' % id
            )


def aggregate(tweets, ID):
    dbPath = path + 'aggregate-%s.db' % ID
    print dbPath
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()
    c.execute(tweetE)
    c.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets )
    conn.commit()
    conn.close()

def main(ID, path):
	## all dbs in the folder
	files = getFiles(path, '.db', full=True)

	## get all tuples
	rows = []
	for f in files:
	    rows+=getSQLiteTweets(f)


	# parse tuples
	ID = 'first_try'
	parserRows = [parseRow(row, id=ID) for row in rows]

	tweetE = '''CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, 
	                                               timestamp INTEGER, 
	                                               lon REAL, lat REAL, 
	                                               tweet TEXT, 
	                                               user_id INTEGER, 
	                                               rtwts INTEGER,
	                                               fvrts INTEGER,
	                                               application TEXT,
	                                               source TEXT )'''
	
	aggregate(parserRows, ID)

if __name__ == '__main__':
	main(sys.argv[1], sys.argv[2])