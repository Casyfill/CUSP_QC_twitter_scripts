#!/usr/bin/env python
#-*- coding: utf-8 -*-

import pandas as pd

import matplotlib.pyplot as plt

dem = pd.read_csv('demographic/demographic.csv', low_memory=False)
dem.drop(dem.index[0], inplace=1)


def compare(featureSet, partition, ax=None, data=dem):
    '''
    compare clusters of zipcodes using specific feature

    featureSet - list of strings, name of the columns in data to operate on
    partition - df, which has two columns with the following names: zip, label
    ax - ax, subplot to populate the plot, optional
    data - df, dataset to retrieve feature from

    TODO:
        - plot size
        - legend minimize
        - title
        - comparison presets

    '''

    for feature in featureSet:
    	if feature not in data.columns:
            raise IOError('Feature %s not in the dataset' % feature)

    if 'zip' not in partition.columns or 'label' not in partition.columns:
        raise IOError("can't see zip in the partition")

    nClstrs = len(partition['label'].unique())  # number of clusters

    # create ax if there is no passed one (for subplots)

    if not ax:

        width = min(nClstrs * 2, 18)
        fig, ax = plt.subplots(figsize=(width, 5))

        plt.axis('off')


    # get only features and zipcode. rename zipcode
    zn = 'ZIP Code Tabulation Area (5-digit)'
    fset = data[[zn] + featureSet].rename(columns={zn: 'zip'})

    # group and calculate
    d = partition.merge(fset, how='left', on='zip')[['label'] + featureSet]
    d = d.groupby('label').agg(sum)
    x = d.divide(d.sum(1), 0)

    x.plot(kind='bar', cmap='spectral', ax=ax, stacked=True)

    return x
