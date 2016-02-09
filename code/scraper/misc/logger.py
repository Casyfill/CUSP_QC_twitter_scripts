#!/usr/bin/env python
#-*- coding: utf-8 -*-

import logging
import sys

class SystemLog(object):
    def __init__(self, name=None):
        self.logger = logging.getLogger(name)

    def write(self, msg, level=logging.INFO):
        self.logger.log(level, msg)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()




def config_logger():
	sys.stderr = SystemLog('stderr')
    logging.basicConfig(filename = './logs/'  + datetime.datetime.now().strftime('%b_%d_%y_%H_%M') + '.out', filemode = 'a', format = '%(asctime)s, %(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S', level = logging.DEBUG)


def save_start_status(mainPath, credentials, place):
  '''general scraper log'''
  timestamp = datetime.datetime.now().strftime('%b_%d_%y_%H_%M')

  slog = pd.read_csv(mainPath +'/status.csv')  
  
  slog= slog.append(pd.Series({'timestamp': timestamp,
                   'credentials':credentials,
                   'place':place,
                   'status':'start'}), ignore_index=1)
    
  slog.to_csv(PWD +'/status.csv', index=0)

  return timestamp 


def save_end_status(mainPath, credentials,place,timestamp):
  print "%s finished! cred=%s" %(place, credentials) 
  slog = pd.read_csv(mainPath +'/status.csv')
  slog.status[(slog.place==place) & (slog.credentials==credentials) & (slog.timestamp==timestamp)] = 'finished'
  slog.to_csv(PWD +'/status.csv', index=0)


