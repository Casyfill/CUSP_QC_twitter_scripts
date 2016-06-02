#!/usr/bin/env python
#-*- coding: utf-8 -*-
import datetime
import pandas as pd


def _avWeek(df, tcol='ts', ccol='id', label=None):
    
    s = df[[tcol, ccol]].rename(columns={tcol: 'ts', 
                                         ccol: 'id'})  # rename to convention

    s = df[['id', 'ts']].set_index('ts').resample(
        '15Min', how='count').reset_index()
    s['id'] = s['id'].astype(float)

    s['ts'] = s.ts.apply(lambda x: datetime.datetime(year=2015, month=1,
                                                     day=(x.weekday() + 1),
                                                     hour=x.hour,
                                                     minute=x.minute))

    if not label: label = ccol
    return s.groupby(['ts']).agg('mean').rename(columns={'id':label})


def averageWeek(df, ax, tcol='ts', ccol='id', label=None, treshold=0, normalize=True, verbose=False, **kwargs):
    '''calculate average week on ts'''
    s = _avWeek(df, tcol, ccol, label)

    if s.id.sum() >= treshold:

        if normalize:
            sNorm = 1.0 * s / s.sum()
        else:
            sNorm = s

        #sNorm.rename(columns={'id': label}, inplace=1)
        sNorm.plot(ax=ax, legend=False, **kwargs)

        return sNorm.rename(columns={'id': label})

    else:
        if verbose:
            print name, 'didnt pass treshhold:', s['id'].sum()

        pass

def _genBulkWeeks(df, attr, th=0):
    
    weeks = []
    for name, g in df.groupby(attr):
        #        zs = averageWeek(g, ax=ax, label=name, alpha=.5, treshold=th, **kwargs)
        zs = _avWeek(df, tcol='ts', ccol='id', label=name)
        weeks.append(zs)
    
    return pd.concat(weeks, axis=1)
    

def bulkWeeks(df, attr, title='', av=False, th=0, legend=False, **kwargs):
    fig, ax = plt.subplots(figsize=(18,6))

    data = _genBulkWeeks(df, attr, th)
    
    if av:
        d = data.mean(axis=1)
        (1.0*d/d.sum()).plot(ax=ax, lw=1.4, color='k', label='Average')


    ax.set_title('%s, treshold=%d' % (title,th), fontsize=15);
    
    if legend:
        ax.legend()

    labels = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    dates = [datetime.datetime(year=2015, month=1, day=i, hour=0, minute=0) for i in range(1,8)]

    ax.set_xticklabels([],minor=False) # the default
    ax.set_xticklabels(labels,minor=True)

    for d in dates:
        ax.axvline(x=d, ymin=0, ymax=1, alpha=.5, linewidth=4)

    return data

def normaliseTimeseries(df, transpose=True):
    '''transpose and normilize timeseries by themself
       removing the median and dividing by sdt'''
    
    if transpose:
        df = df.T
    
    return (df - df.mean(0))/df.std(0)

def nColors(k=2, cmap='spectral'):

    from pylab import get_cmap
    
    if type(cmap) == str:
        cm = get_cmap(cmap)
        colors = [cm(1.*i/(k-1)) for i in range(k)]
    elif cmap==None:
        colors = ['k']
    else:
        colors = cmap
        
    return colors
    
    
    
    