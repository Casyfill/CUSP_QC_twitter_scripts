import sqlite3
from auth import scraperID
import datetime

def setup():
	ID = scraperID + '2_' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
	path = 'data/%s.db' % ID
	
	conn = sqlite3.connect(path)
	print path

	c = conn.cursor()

	# c.execute('''CREATE TABLE tweets (id INTEGER PRIMARY KEY, tweet TEXT, geo TEXT, timestamp TEXT, user_id INTEGER, raw TEXT)''')
	c.execute('CREATE TABLE IF NOT EXISTS tweets (id INTEGER PRIMARY KEY, timestamp INTEGER, lon REAL, lat REAL, tweet TEXT, user_id INTEGER, rtwts INTEGER, fvrts INTEGER, application TEXT, source TEXT, raw TEXT )')
	c.execute('''CREATE TABLE users (id INTEGER, timestamp TEXT, raw TEXT, CONSTRAINT pk PRIMARY KEY (id, timestamp))''')

	conn.commit()
	conn.close()
	return ID


if __name__ == '__main__':
	setup()