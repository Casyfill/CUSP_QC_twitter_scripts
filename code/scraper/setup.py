import sqlite3

import os
import glob
import time
 

def setup():

	fecha = 'data/twitter'+time.strftime("%d-%m-%y-%H:%M")+'.db'
	conn = sqlite3.connect(fecha)

	c = conn.cursor()

	c.execute('''CREATE TABLE tweets (id INTEGER PRIMARY KEY, tweet TEXT, geo TEXT, timestamp TEXT, user_id INTEGER, raw TEXT)''')
	c.execute('''CREATE TABLE users (id INTEGER, timestamp TEXT, raw TEXT, CONSTRAINT pk PRIMARY KEY (id, timestamp))''')

	conn.commit()
	conn.close()

	return fecha

if __name__ == '__main__':
	setup()