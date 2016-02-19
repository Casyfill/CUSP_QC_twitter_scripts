#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import geopandas as gp
from shapely.geometry import Point


def getSQLiteTweets(path, cols):
    '''gets all data as tuple'''
    query = 'SELECT %s FROM tweets' % ', '.join(cols)

    conn = sqlite3.connect(path)
    data = conn.cursor().execute(query).fetchall() ## all but raw data
    conn.close()
    return data

def getDF(path):
    columns = ('id','timestamp','lon', 'lat', 'tweet', 'user_id', 'rtwts', 'fvrts', 'application', 'source')
    data = getSQLiteTweets(path, columns)
    
    return pd.DataFrame(data, columns=columns )


def toGeoDataFrame(df, lat='Latitude',lon='Longitude'):
    '''dataframe to geodataframe'''
    df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
    df = gp.GeoDataFrame(df)
    df.crs = {'init': 'epsg:4326', 'no_defs': True}
    return df 
    