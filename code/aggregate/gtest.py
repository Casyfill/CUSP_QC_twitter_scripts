#!/usr/bin/env python
#-*- coding: utf-8 -*-


from sqlalchemy import create_engine
import pandas as pd



		

def main(path):
	engine = create_engine('sqlite:///%s' % path)
	columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')
	
	with engine.connect() as conn, conn.begin():
		
		chunks = pd.read_sql_table('tweets', conn, columns=columns, chunksize=100)
		return reduce(lambda x,y: x + y.shape[0], chunks, 0)
	

	




if __name__ == '__main__':
	print main('merged1.db')