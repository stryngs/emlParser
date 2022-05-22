#!/usr/bin/python3
import csv
import datetime
import json
import os
import pandas as pd
import sqlite3 as lite
import time
import warnings
from email import policy
from email.parser import BytesParser

## os prep
tStart = time.time()
if os.path.isfile('emailParseErrors.lst'):
    os.remove('emailParseErrors.lst')
if os.path.isfile('csvWriteErrors.lst'):
    os.remove('csvWriteErrors.lst')
if os.path.isfile('eml.sqlite3'):
    os.remove('eml.sqlite3')
if os.path.isfile('tmp.csv'):
    os.remove('tmp.csv')

## Parse prep
eList = []
brokenCounts = []
cnt = 1
stdCnt = 1000

## Cycle through emails
for eml in os.listdir('emails'):

    ## Obtain the email message
    proceed = False
    with open('emails/{0}'.format(eml), 'rb') as emlFile:
        msg = BytesParser(policy = policy.default).parse(emlFile)

        ## Deal with parsing failures
        if len(msg.keys()) == 0:
            brokenCounts.append(eml)
            proceed = False

        ## Parsing worked, grab the message body
        else:
            proceed = True
            try:
                txt = msg.get_body(preferencelist = ('plain')).get_content()
            except:
                txt = None

    ## Parsing worked, grab the other pertinent information
    if proceed is True:

        ## Date
        dt = msg.get('Date')

        ## Subject
        subj = msg.get('subject')

        ## From
        _FROM = msg.get('From')
        if _FROM is not None:
            _dfrom = _FROM.addresses[0].display_name
            _from = _FROM.addresses[0].addr_spec
        else:
            _dfrom = None
            _from = None

        ## Cc
        try:
            _CC = [i for i in msg.get('Cc').addresses]
            _cc = '\n'.join([i.addr_spec for i in _CC])
            _dcc = '\n'.join([i.display_name for i in _CC])
        except:
            _cc = None
            _dcc = None

        ## To
        _to = None
        _dto = None
        try:
            _TO = [i for i in msg.get('To').addresses]
            _to = 1
        except:
            _to = None
            _dto = None

        ## Iterate through multiple recipients and update eList
        if _to == 1:
            for addr in _TO:
                _to = addr.addr_spec
                _dto = addr.display_name
                eList.append((eml, dt, subj, _from, _to, _cc, _dfrom, _dto, _dcc, txt))

        ## Update eList with the singular recipient
        else:
            eList.append((eml, dt, subj, _from, _to, _cc, _dfrom, _dto, _dcc, txt))

    ## Let the user know the status and update the count
    if cnt % stdCnt == 0:
        print(cnt, time.time())
    cnt += 1

## Notate failed email parsing
with open('emailParseErrors.lst', 'w') as oFile:
    for broken in brokenCounts:
        oFile.write(broken + '\n')

## Write a temporary CSV
hdrs = ['_eml', '_time', '_subj', '_from', '_to', ' _cc', '_dfrom', '_dto', '_dcc', '_msg']
with open('csvWriteErrors.lst', 'a') as eFile:
    with open('tmp.csv', 'w', encoding = 'utf-8') as oFile:
        writer = csv.writer(oFile)
        writer.writerow(hdrs)
        for e in eList:
            try:
                writer.writerow(e)
            except:
                eFile.write(e[0] + '\n')

## Convert to sql
warnings.filterwarnings('ignore')
con = lite.connect('eml.sqlite3')
df = pd.read_csv('tmp.csv', encoding = 'utf-8')
df.to_sql('eml', con, index = False)
con.commit()
con.close()

## Cleanup
os.remove('tmp.csv')
print(' Total runtime of ' + str(int(time.time() - tStart)) + ' seconds\n')
