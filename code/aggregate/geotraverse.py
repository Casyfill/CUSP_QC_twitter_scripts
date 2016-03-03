#!/usr/bin/env python
#-*- coding: utf-8 -*-

import pandas as pd

import geopandas as gp
from shapely.geometry import Point
from geopandas.tools import sjoin
# import pickle
# import sys

import os


import multiprocessing as mp
from sqlalchemy import create_engine

def getFiles(folder, format, full=True, filtr=None):
    for f in os.listdir(folder):
        if f.endswith(format):
            if filtr==None or filtr not in f:
                if full:
                    yield os.path.join(folder, f)
                else:
                    yield f

def mostRecentFile(path, frmt='.db'):
    '''returns most recent .db file in folder'''

    def reduceRecent(current, newPath):
        d = os.stat(newPath).st_ctime
        if d > current[1]:
            return (newPath,d)

    path, time = reduce(reduceRecent, getFiles(path, frmt), (None, 0))
    return  path




def correctTime(data):
	 ## UTS - 5h NYC
	data['ts'] = pd.to_datetime(data.timestamp, unit='s') - datetime.timedelta(hours=5)
	return data

def toGeoDataFrame(df, lat='Latitude',lon='Longitude'):
	'''dataframe to geodataframe'''
	df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
	df = gp.GeoDataFrame(df)
	df.crs = {'init': 'epsg:4326', 'no_defs': True}
	return df 



def geoCode(df):
	'''all geoprocessing part'''
	df = df[(pd.notnull(df.lat)) & (pd.notnull(df.lon)) ] # filter tweets without geocoordinates

	gdf = toGeoDataFrame(df, lat='lat', lon='lon')
	zips = gp.read_file('DATAVAULT/misc/zipcodes.geojson')[['geometry', 'postalCode']]

	return sjoin(gdf, zips, how="left", op='within')

def job(chunk):
	'''main job to delegate'''
	engine = create_engine('sqlite:///tempGeodb.db')
	toSQL(correctTime(geoCode(chunk)), engine)

def toSQL(df, engine):

	conn  = engine.connect()
	conn.begin()
	df.to_sql('tweets', conm, flavor='sqlite', if_exists='append', index=False)
	conn.close()

	# with engine.connect() as conn:
	# 	with conn.begin():
	# 		df.to_sql('tweets', conm, flavor='sqlite', if_exists='append', index=False)



def main():
	fPath =  mostRecentFile('DATAVAULT/OUT/', '.db')
	print fPath


	engine = create_engine('sqlite:///%s' % fPath)
	columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')
	
	conn = engine.connect()
	conn.begin()
	chunks = pd.read_sql_table('tweets', conn, columns=columns, chunksize=100)

	pool = mp.Pool()
	pool.map(job, chunks)
	conn.close()


if __name__ == '__main__':
	
	main()