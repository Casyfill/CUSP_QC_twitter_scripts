#!/usr/bin/env python
#-*- coding: utf-8 -*-
import pandas as pd
from scipy.cluster.vq import kmeans2
import matplotlib.pyplot as plt
import Pycluster


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


def clusterAndPlot(df, k, height=10, engine='PyCluster', cmap='spectral'):
    '''calculate and plot kmean clustering'''
    fig, axes = plt.subplots(k + 1, figsize=(18, height),
                             sharex='all', sharey='all')

    if engine == 'scipy':
        centroids, label = kmeans2(df, k, iter=100, thresh=1e-05)
    else:
        labels, error, nfound = Pycluster.kcluster(df, k)
    df['label'] = labels

    colors = nColors(k=k, cmap=cmap)

    # one by one
    for l, g in df.groupby('label'):
        g.T.plot(ax=axes[l], legend=0, c=colors[l], alpha=.2)
        axes[l].set_title('cluster %d, %d zipcodes' % (l, len(g)))

        pd.Series(g.mean(0)).plot(
            ax=axes[-1], label='cluster %d' % (l), c=colors[l])

    #     plt.legend()
    return df
