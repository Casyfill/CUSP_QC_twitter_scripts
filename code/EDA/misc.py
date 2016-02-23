#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import geopandas as gp
from shapely.geometry import Point
import os


def getFiles(folder, format, full=True, filtr=None):
    for f in os.listdir(folder):
        if f.endswith(format):
            if filtr==None or filtr not in f:
                if full:
                    yield os.path.join(folder, f)
                else:
                    yield f


def mostRecentFile(path):
    '''returns most recent .db file in folder'''

    def reduceRecent(current, newPath):
        d = os.stat(newPath).st_ctime
        if d > current[1]:
            return (newPath,d)


    path, time = reduce(reduceRecent, getFiles(path, 'db'), (None, 0))

    return  path

def getSQLiteTweets(path, cols):
    '''gets all data as tuple'''
    query = 'SELECT %s FROM tweets' % ', '.join(cols)

    conn = sqlite3.connect(path)
    data = conn.cursor().execute(query).fetchall() ## all but raw data
    conn.close()
    return data

def getDF(path):
    
    engine = create_engine('sqlite:///%s' % path)
    columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')

    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_table('tweets', conn, columns=columns)

    data['ts'] = pd.to_datetime(data.timestamp, unit='s') - datetime.timedelta(hours=5) ## UTS - 5h NYC
    return misc.toGeoDataFrame(data, lat='lat',lon='lon')  



def toGeoDataFrame(df, lat='Latitude',lon='Longitude'):
    '''dataframe to geodataframe'''
    df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
    df = gp.GeoDataFrame(df)
    df.crs = {'init': 'epsg:4326', 'no_defs': True}
    return df 
    