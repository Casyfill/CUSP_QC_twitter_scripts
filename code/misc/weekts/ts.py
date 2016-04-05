#!/usr/bin/env python
#-*- coding: utf-8 -*-
import datetime
import pylab as plt
import pandas as pd


def averageWeek(df, ax=None, tcol='ts', ccol='application',
                label=None, treshold=0, normalize=True,
                verbose=False, **kwargs):
    '''calculate average week on ts'''

    s = df[[tcol, ccol]].rename(
        columns={tcol: 'ts', ccol: 'id'})  # rename to convention

    s = s[['id', 'ts']].set_index('ts').resample(
        '15Min', how='count').reset_index()
    s['id'] = s['id'].astype(float)

    s['ts'] = s.ts.apply(lambda x: datetime.datetime(year=2015, month=1,
                                                     day=(x.weekday() + 1),
                                                     hour=x.hour,
                                                     minute=x.minute))

    # s = s[s['id']>0] # remove zero 15m samples should concider working on
    # this
    s = s.groupby(['ts']).agg('mean')

    if not ax:
        fig, ax = plt.subplots(figsize=(18, 6))

    if not label:
        label = ccol

    if s.id.sum() >= treshold:

        if normalize:
            sNorm = 1.0 * s / s.sum()
        else:
            sNorm = s

        sNorm.rename(columns={'id': label}, inplace=1)
        sNorm.plot(ax=ax, legend=False, **kwargs)

        return sNorm.rename(columns={'id': label})

    else:
        if verbose:
            print label, 'didnt pass treshhold:', s['id'].sum()

        pass


def bulkWeeks(df, attr, title='', av=False, th=0, legend=False, **kwargs):
	'''
	create and plot average weekSeries using attr column for partition
	'''
	fig, ax = plt.subplots(figsize=(18, 6))

	weeks = []

	for name, g in df.groupby(attr):
		zs = averageWeek(g, ax=ax, label=name, alpha=.5, treshold=th, **kwargs)
		weeks.append(zs)

	data = pd.concat(weeks, axis=1)
	if av:
		d = data.mean(axis=1)
        (1.0 * d / d.sum()).plot(ax=ax, lw=1.4, color='k', label='Average')

	ax.set_title('%s, treshold=%d' % (title, th), fontsize=15)

	if legend:
		ax.legend()

	labels = ['Monday', 'Tuesday', 'Wednesday',
			  'Thursday', 'Friday', 'Saturday', 'Sunday']

	dates = [datetime.datetime(
		year=2015, month=1, day=i, hour=0, minute=0) for i in range(1, 8)]

	ax.set_xticklabels([], minor=False)  # the default
	ax.set_xticklabels(labels, minor=True)

	for d in dates:
		ax.axvline(x=d, ymin=0, ymax=1, alpha=.5, linewidth=4)

	return data
