from misc.auth import getTwitter
from misc.setup import setup
from misc.logger import getLogger
# from misc import mailer

import time
import datetime
# import csv
import json
import sqlite3
import signal
import sys
# import os
import re


def getSource(txt):
    sourcer = re.compile('(?:.*>)(.*)(?:<\/a>)')
    x = sourcer.search(txt)
    if x:
        return x.groups()[0]
    else:
        return 'unknnown'


def main():
    '''main process'''
    logger = getLogger()

    # create DB if does not exist
    ID = setup()

    # Connect to DB
    logger.info('Connecting to %s.db' is ID)
    conn = sqlite3.connect('data/%s.db' % ID)

    def signal_handler(signal, frame):
        # Close connection on interrupt
        conn.close()
        sys.stdout.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    logger.info('Getting twitter connection')
    twitter = getTwitter()

    nodes = [
        #{'geocode': '40.783288,-73.967090,7mi', 'since': '0'},
        #{'geocode': '40.729992,-73.993841,7mi', 'since': '0'},
        #{'geocode': '40.830778,-73.942806,7mi', 'since': '0'}
        {'geocode': '40.830956,-73.910179,7mi', 'since': '0'},
        {'geocode': '40.663972,-73.956871,8mi', 'since': '0'},
        {'geocode': '40.688708,-73.779544,8mi', 'since': '0'},
        {'geocode': '40.580584,-74.152908,9mi', 'since': '0'}
    ]

    # Total of 20 seconds sleep between rounds
    sleep = 20.
    # today = datetime.datetime.today()

    while True:
        for node in nodes:
            # Execute Query
            try:
                t = twitter.search.tweets(geocode=node['geocode'], result_type='recent',
                                          count=100, since_id=node['since'])
            except Exception, e:
                logger.info('Error getting tweet: %s' % e)
                # Could be twitter is overloaded, sleep for a minute before
                # starting again
                time.sleep(60)
                continue

            # Update since
           # node['since'] = t['search_metadata']['max_id_str']

            # Print status
            # print node['geocode'], len(t['statuses']),
            # str(datetime.datetime.now())

            # Go through the results and create arrays to add to DB
            tweets = []
            users = []
            logger.info('Collecting geotagged tweets')
            for status in t['statuses']:
                if status['geo'] != None:

                    user = status['user']
                    del status['user']

                    timestamp = int(datetime.datetime.strptime(
                        status['created_at'],
                        '%a %b %d %H:%M:%S +0000 %Y'
                    ).strftime("%s"))

                    tweets.append((
                        status['id'],
                        timestamp,
                        status['geo']['coordinates'][0],
                        status['geo']['coordinates'][1],
                        status['text'],
                        user['id'],
                        status['retweet_count'],
                        status['favorite_count'],
                        getSource(status['source']),
                        ID,
                        json.dumps(status)
                    ))
                    users.append((
                        user['id'],
                        timestamp,
                        json.dumps(user)
                    ))

                else:
                    pass

            # Add to DB
            try:
                cursor = conn.cursor()
                cursor.executemany('INSERT OR IGNORE INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tweets)
                cursor.executemany(
                    'INSERT OR IGNORE INTO users VALUES (?, ?, ?)', users)
                conn.commit()

                node['since'] = t['search_metadata']['max_id_str']
                logger.info('Saved tweets to db: %d' % len(tweets))

            except Exception, e:
                logger.info('Failed saving tweets, reconnecting: %s' % e)
                time.sleep(60)
                conn = sqlite3.connect('data/%s.db' % ID)

            # Sleep between nodes
            time.sleep(sleep / len(nodes))

        sys.stdout.flush()
        logger.info('flushed connection')

if __name__ == '__main__':
    main()
