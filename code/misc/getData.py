#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Philipp Kats'
__version__ = 1.0

import pandas as pd
from sqlalchemy import create_engine
import os


def _getFiles(folder, format, full=True, filtr=None):
    for f in os.listdir(folder):
        if f.endswith(format):
            if filtr is None or filtr not in f:
                if full:
                    yield os.path.join(folder, f)
                else:
                    yield f


def _mostRecentFile(path, frmt='.db'):
    '''returns most recent .db file in folder'''

    def _reduceRecent(current, newPath):
        d = os.stat(newPath).st_ctime
        if d > current[1]:
            return (newPath, d)

    path, time = reduce(_reduceRecent, _getFiles(path, frmt), (None, 0))
    return path

def get_con(path, poolsize):
    return create_engine('sqlite:///%s' % path, poolsize=poolsize)

def get_data(columns=['ts', ], path='../../../QC_data/DATAVAULT/OUT/', recent=True, fltr=None, chs=None, verbose=True):
    '''return queried data as dataframe

    return queried data as dataframe,
    witout any calculations: 
    just selected columns.

    if chs=None, returns single pd.DataFrame
    else, returns iterator of chunks, with max size = chs

    Args:
        columns(list): list of column names, default to only "ts" (timestamp) column
        fPath(str): path to data folder or database
        recent(bool): if True, will treat path as path to folder and look for most recent db file in that folder.
        fltr(str): if needed, add filter SQL string to filter down the database
        chs(int): chunksize, if None will return one dataframe instead

    Returns:
        pd.Dataframe: one dataframe or chunks'''

    # print os.getcwd()
    if recent:
        path = _mostRecentFile(path, frmt='.db')

    engine = create_engine('sqlite:///%s' % path)

    Q = 'SELECT {0} FROM tweets'.format(', '.join(columns))
    if fltr:
        Q = ' '.join((Q, fltr))

    if verbose:
        print('executing {0} on database\n{1}'.format(Q, path))

    if chs:
        return pd.read_sql_query(Q, con=engine, chunksize=chs)

    return pd.read_sql_query(Q, con=engine)
