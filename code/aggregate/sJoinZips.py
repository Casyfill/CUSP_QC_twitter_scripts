#!/usr/bin/env python
#-*- coding: utf-8 -*-


import pandas as pd
import geopandas as gp
from shapely.geometry import Point
from geopandas.tools import sjoin


def getSQLiteTweets(path):
    '''gets all data as tuple'''
    conn = sqlite3.connect(path)
    data = conn.cursor().execute('SELECT * FROM tweets').fetchall() ## all but raw data
    conn.close()
    return data

def getDF(path):
    data = getSQLiteTweets(path)
    columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')
    
    return pd.DataFrame(data, columns=columns )
    

def toGeoDataFrame(df, lat='Latitude',lon='Longitude'):
    '''dataframe to geodataframe'''
    df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
    df = gp.GeoDataFrame(df)
    df.crs = {'init': 'epsg:4326', 'no_defs': True}
    return df 

def sJoinZips(path):
	'''give a zip attribute for each tweet'''
	zipPath = 'zips.json'
	gdf = toGeoDataFrame(getDF(path), lat='lon',lon='lat')
	zips = gp.read_file(zipPath)[['geometry', 'postalCode']]

	save_to_DB

if __name__ == '__main__':
		sJoinZips()	

