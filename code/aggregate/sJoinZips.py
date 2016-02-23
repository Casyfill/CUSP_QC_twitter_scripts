#!/usr/bin/env python
#-*- coding: utf-8 -*-


import pandas as pd
import geopandas as gp
from sqlalchemy import create_engine

from misc import toGeoDataFrame

def getDF(path):
	'''return geodataframe'''
	# FIXIT: repair timestamp parsing

	engine = create_engine('sqlite:///%s' % path)
	columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')

	with engine.connect() as conn, conn.begin():
	    data = pd.read_sql_table('tweets', conn, columns=columns)

	data['ts'] = pd.to_datetime(data.timestamp, unit='s') - datetime.timedelta(hours=5) ## UTS - 5h NYC
	return misc.toGeoDataFrame(data, lat='lat',lon='lon')  


def sJOINT(gdf):
	'''attaches zipcode label to each point'''

	zipPath = '/misk/zipcode_tabular.geojson'
	zips = gp.read_file(zipPath)[['geometry', 'postalCode']]

	return sjoin(gdf, zips, how="left", op='within')

def writeSQL(gdf, path):
	dbPath = path.replace('.db','_zips.db')
	engine = create_engine('sqlite:///%s' % dbPath)
	columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')

	with engine.connect() as conn, conn.begin():
	gdf[[x for x in gdf.columns.tolist() if x!='geometry']].to_sql('tweets', con, flavor='sqlite', schema=None, if_exists='fail')
	
	print 'Done!\n\n', dbPath


def main(path):
	
	gdf = getDF(path)
	result = sJOINT(gdf)
	writeSQL(result, path)

if __name__=='__main__':
    if len(sys.argv)<2:
        sys.stderr.write('USAGE: python %s <DATABASE FILE>\n' % sys.argv[0])
        sys.exit(1)
        
        main(sys.argv[1])
        