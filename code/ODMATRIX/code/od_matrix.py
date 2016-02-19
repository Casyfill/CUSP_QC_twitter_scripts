#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = "Philipp Kats"
__date__ = "2016_02_19"


import sys
import pandas as pd



def selfMatrix(l):
    '''all possible pairs of elements, once '''

    for i, l1 in enumerate(l):
        for l2 in l[i:]:
            yield (l1, l2)

def framer(df):
	'''every user presented as a row, every zipcode - as a column.
	if user never posted in the zipcode, cell will be filled with NaN
	therefore, when we filter the DF by df[(pd.notnull(pd[zipcode1])) & (pd.notnull(pd[zipcode2]))],
	we garantee that this user was in both zipcodes and we can get the sum of filtered rows'''


	g = df[['user_id','postalCode','id']].groupby(['user_id', 'postalCode']).agg('count')
	frame = g.unstack()
	frame.columns = [int(x) for x in frame.columns.droplevel().tolist()] 
	return frame

def getValues(df, zip1, zip2):
    '''returns summ of values for both zipcodes
    only from users who were at both zipcodes,
    as two (zip, value) tuples'''
    
    return df[(pd.notnull(frame[zip1]))&(pd.notnull(frame[zip2]) )][[zip1, zip2]].sum().iteritems()
    

def main(path):

	df = pd.read_csv(path, index_col=0)
	df = df[pd.notnull(df.postalCode)]

	# frame
	frame = framer(df)
	# zipcodes list
	zipcodes = df.postalCode.dropna().astype(int).unique().tolist()

	# matrix
	matrix = []

	for pair in selfMatrix(zipcodes):
	    v1, v2 = getValues(frame, pair[0], pair[1])
	    matrix.append({'zip1':v1[0], 'zip1_value':v1[1], 
	                   'zip2':v2[0], 'zip2_value':v2[1] })


	matrixDF = pd.DataFrame(matrix)
	matrixDF.to_csv('matrix.csv')
	





if __name__ == '__main__':
	if len(sys.argv)<2:
        sys.stderr.write('USAGE: python %s <INPUT_CSV>\n' % sys.argv[0])
        sys.exit(1)
	main(sys.argv[1])