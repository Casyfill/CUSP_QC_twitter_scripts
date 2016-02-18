#!/usr/bin/env python
#-*- coding: utf-8 -*-

import requests
from auth import mailCredentials, scraperID
import datetime


def send_message(subject, text, recipients):
    return requests.post(
        "https://api.mailgun.net/v3/mg.foobar.com/messages",
        auth=("api", mailCredentials['apiKey']),
        data={"from": mailCredentials['address'],
              "to": recipients,
              "subject": subject,
              "text": text})


def send_stats(cursor):
  '''sends daily stats from scraper'''
  today = datetime.datetime.today().strftime('%Y-%m-%d')
  s = 'DO server is working fine. stats unknown'
  # s = 'Tweets: ' + cursor.execute('SELECT COUNT(*) FROM tweets').fetchone()[0] + '\nUnique Users: ' + cursor.execute('SELECT COUNT(distinct id) FROM users').fetchone()[0]

  send_message('%s daily stats: Twitter scraping' % today, s, mailCredentials['recipients'])

def send_welcoming():
  s = '%s deployed and starts working!\n ' % scraperID
  send_message(s, s, mailCredentials['recipients'])

