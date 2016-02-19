#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = "Philipp Kats"
__date__ = "2016_02_19"


import sys
import sqlite3
import pandas as pd
import geopandas as gp
from geopandas.tools import sjoin
from shapely.geometry import Point

def getSQLiteTweets(path, cols):
    '''gets all data from sqlite as tuple'''
    query = 'SELECT %s FROM tweets' % ', '.join(cols)

    conn = sqlite3.connect(path)
    data = conn.cursor().execute(query).fetchall() ## all but raw data
    conn.close()
    return data

def getDF(path):
	'''all data as pandas df'''
    columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')
    data = getSQLiteTweets(path, columns)
    
    return pd.DataFrame(data, columns=columns )


def toGeoDataFrame(df, lat='Latitude',lon='Longitude'):
    '''dataframe to geodataframe'''
    df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
    df = gp.GeoDataFrame(df)
    df.crs = {'init': 'epsg:4326', 'no_defs': True}
    return df 
    

def main(dbPath, zipPath):
	'''generates sintetic csv 
	from master_df with zipcodes'''

	gdf  = toGeoDataFrame(getDF(dbPath), lat='lon',lon='lat')  ### mesed with lat_long
	zips = gp.read_file(zipPath)[['geometry', 'postalCode']]

	datum = sjoin(gdf, zips, how="left")[['timestamp', 'postalCode', 'user_id', 'id']]
	datum = datum[pd.notnull(datum.postalCode)] # remove failed sjoin
	datum.to_csv('sintetic.csv')



if __name__ == '__main__':
	if len(sys.argv)<3:
        sys.stderr.write('USAGE: python %s <INPUT_DB> <INPUT_ZIPJSON>\n' % sys.argv[0])
        sys.exit(1)
	
	main(sys.argv[1], sys.argv[2])