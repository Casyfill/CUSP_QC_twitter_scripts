#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__='Philipp Kats'
__version__=1.0

import pandas as pd


def utc_to_tz(col, tz='US/Eastern', unix=False):
    '''convert datetime series from UTC to selected timezone
    
    Args:
        col(pd.Series): series of datetime stamps
        tz(str): timezone string for tz to convert time into
        unix(bool): if true, column will be treated as unix timestamp
    
    Returns:
        pd.Series: similar pd.series of adjusted timestamps'''
    if unix:
        col = pd.to_datetime(col, unit='s', utc=True)
    
    ti = pd.DatetimeIndex(pd.to_datetime( col , infer_datetime_format=True) )
    return ti.tz_localize('Utc').tz_convert(tz)
    
    
    
    