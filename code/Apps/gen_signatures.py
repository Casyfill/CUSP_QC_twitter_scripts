#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from sys import argv
from misc.timeseries import _genBulkWeeks
from misc.getData import get_data
from misc.correctTime import utc_to_tz

def getApps(N):
    '''get top apps'''
    df = pd.read_csv('apps_all.csv')
    return df.head(N)['application'].tolist()

#!!!
def getPostals():
    df = pd.read_csv('postalCodes.csv')
    return df['postalCode'].tolist()


def getSignature(postalCode, apps):
    '''get average week fo each app in list
    for specific postalCode, and concat them in one row'''
    
    df = get_data(columns=['timestamp','application','id'], 
                  path='../../../QC_data/DATAVAULT/OUT/2015_10_30_all.db',
                  recent=False,
                  fltr=' WHERE postalCode = {0};'.format(postalCode),
                  verbose=False)
    
    print '{1}: {0}'.format(len(df),postalCode)
    
    df['ts'] = utc_to_tz(df['timestamp'], unix=True)
    df = df[df.application.isin(apps)]
    df = _genBulkWeeks(df, 'application', th=0)
    
    

    return df.T.stack()


def main():
    '''generate week signature'''

    zips = getPostals()
    apps = getApps(15)
    
    def get_reduce(x,y):
        try:
            z1 = int(y)
            a = getSignature(y, apps=apps)
            return x.append(y,1)
        except:
            return x

    
    s = reduce(get_reduce, zips)   
            
    a.to_csv('experiment.csv')

if __name__ == '__main__':
    main()
