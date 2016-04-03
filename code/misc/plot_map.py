#!/usr/bin/env python
#-*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt


def visualizeZipPartition(partition, zips, key='postalCode',
						  cmap='spectral', title='Zipcode partition'):
	'''
	visualise zipcode NYC map
	'''

	fig, ax = plt.subplots(figsize=(18, 18))
	p = pd.Series(partition).reset_index().rename(columns={'index': 'postalCode',
															0: 'part'})

	z = zips.merge(p, on=key, how='left')
	z.plot(ax=ax, alpha=0)
	z[pd.notnull(z['part'])].plot(column='part', ax=ax, cmap=cmap)

	plt.title(title, fontsize=18)
	plt.axis('off')
