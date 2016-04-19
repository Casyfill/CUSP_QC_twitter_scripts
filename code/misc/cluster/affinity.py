#!/usr/bin/env python
#-*- coding: utf-8 -*-

from sklearn.cluster import AffinityPropagation
from sklearn import metrics
import matplotlib.pyplot as plt
import datetime


def nColors(k=2, cmap='spectral'):
    '''
    manual colors retrieval from colormap
    '''
    from pylab import get_cmap

    if type(cmap) == str:
        cm = get_cmap(cmap)
        colors = [cm(1. * i / (k - 1)) for i in range(k)]
    elif cmap is None:
        colors = ['k']
    else:
        colors = cmap

    return colors


def normaliseTimeseries(df, transpose=True):
    '''transpose and normilize timeseries by themself
       removing the median and dividing by sdt'''

    if transpose:
        df = df.T

    return (df - df.mean(0)) / df.std(0)


def affinity(DF):
    '''
    calculate and plot affinity propagation
    ts clustering algoritm, return partition
    '''
    X = normaliseTimeseries(DF)
    A = AffinityPropagation(damping=0.5, max_iter=200, convergence_iter=15)

    af = A.fit(X)

    cluster_centers_indices = af.cluster_centers_indices_
    labels = af.labels_
    n_clusters_ = len(cluster_centers_indices)

    M = metrics.silhouette_score(X, labels, metric='sqeuclidean')
    print('Estimated number of clusters: %d' % n_clusters_)
    print("Silhouette Coefficient: %0.3f" % M)

    fig, axes = plt.subplots(nrows=n_clusters_, figsize=(24, 18), sharex='all')

    colors = nColors(k=n_clusters_, cmap='spectral')
    ticks = ['Monday', 'Tuesday', 'Wednesday',
             'Thursday', 'Friday', 'Saturday', 'Sunday']

    dates = [datetime.datetime(year=2015, month=1, day=i,
                               hour=0, minute=0) for i in range(1, 8)]

    for k, col in zip(range(n_clusters_), colors):
        X.iloc[cluster_centers_indices[k], :].plot(
            lw=1, c=col, label=k, alpha=.5, ax=axes[k])

        X[labels == k].T.plot(lw=.5, c=col, alpha=0.2, ax=axes[k], legend=0)
        axes[k].set_title('cluster %d, %d zipcodes' %
                          (k, len(X[labels == k])), fontsize=16)

        axes[k].set_xticklabels([], minor=False)  # the default
        axes[k].set_xticklabels(ticks, minor=True)
        axes[k].set_yticklabels([], minor=False)

        for d in dates:
            axes[k].axvline(x=d, ymin=0, ymax=1, alpha=.5, linewidth=2)

    plt.tight_layout()

    result = DF.T
    result['label'] = labels
    result.reset_index(inplace=1)
    result.rename(columns={'index': 'postalCode'}, inplace=1)

    return result[['postalCode', 'label']]
