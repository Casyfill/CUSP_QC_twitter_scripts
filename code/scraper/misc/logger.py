#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import datetime


def getLogger():
	''' setup and get logger
	'''

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    date = datetime.datetime.now().strftime('%Y_%m_%d')
    handler = logging.FileHandler('%s_scraping.log' % date)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.info('%s: start logging' % date)
    return logger
