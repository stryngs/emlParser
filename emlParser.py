#!/usr/bin/python3
import csv
import io
import os
import mimetypes
import pandas as pd
import shutil
import sqlite3 as lite
import time
import warnings
from email import policy
from email.parser import BytesParser

## os prep
tStart = time.time()
tVal = tStart
if os.path.isfile('emailParseErrors.log'):
    os.remove('emailParseErrors.log')
if os.path.isfile('csvWriteErrors.log'):
    os.remove('csvWriteErrors.log')
if os.path.isfile('exceptions.log'):
    os.remove('exceptions.log')
if os.path.isfile('eml.sqlite3'):
    os.remove('eml.sqlite3')
if os.path.isdir('attachments'):
    shutil.rmtree('attachments')
os.mkdir('attachments')

## Parse prep
eList = []
brokenCounts = []
cnt = 1
stdCnt = 10000

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

        ## attachment parsing ideas-> https://gist.github.com/dukedougal/9301467
        try:
            hasAttach = 0
            extension = None
            aDir = 'attachments/{0}'.format(eml.split('.')[0])
            for attachment in msg.iter_attachments():
                hasAttach += 1
                fn = attachment.get_filename()
                if fn:
                    extension = os.path.splitext(attachment.get_filename())[1]
                else:
                    extension = mimetypes.guess_extension(attachment.get_content_type())
                    if extension is None:
                        extension = '.txt'
                if len(extension) == 0:
                    extension = '.txt'
                f = io.BytesIO()
                data = attachment.get_content()
                if len(data) > 0:
                    if not os.path.isdir(aDir):
                        os.mkdir(aDir)
                    if fn is None:
                        fn = 'noFilename'
                        if type(data) == str:
                            with open(f'{aDir}/{fn}{extension}', 'a') as sFile:
                                sFile.write(data)
                        else:
                            if extension != '.eml':
                                with open(f'{aDir}/{fn}{extension}', 'ab') as bFile:
                                    bFile.write(data)
                            else:
                                with open(f'{aDir}/{dt}{extension}', 'a') as sFile:
                                    sFile.write(data.as_string())
                    else:
                        if type(data) == str:
                            with open(f'{aDir}/{fn}{extension}', 'w') as sFile:
                                sFile.write(data)
                        else:
                            with open(f'{aDir}/{fn}{extension}', 'wb') as bFile:
                                bFile.write(data)

        except Exception as E:
            with open('exceptions.log', 'a') as oFile:
                oFile.write(f'eml: {eml}\n')
                oFile.write(f'ERROR: {E}\n')
                oFile.write(f'aDir: {aDir}\n')
                oFile.write(f'fn: {fn}\n')
                oFile.write(f'ext: {extension}\n')
                oFile.write('dType: {0}\n\n'.format(type(data)))

        ## Iterate through multiple recipients and update eList
        if _to == 1:
            for addr in _TO:
                _to = addr.addr_spec
                _dto = addr.display_name
                eList.append((eml, dt, subj, _from, _to, _cc, _dfrom, _dto, _dcc, hasAttach, txt))

        ## Update eList with the singular recipient
        else:
            eList.append((eml, dt, subj, _from, _to, _cc, _dfrom, _dto, _dcc, hasAttach, txt))


    ## Let the user know the status and update the count
    if cnt % stdCnt == 0:
        oVal = tVal
        tVal = time.time()
        print(cnt, tVal, tVal - oVal, tVal - tStart)
    cnt += 1

## Notate failed email parsing
with open('emailParseErrors.log', 'w') as oFile:
    for broken in brokenCounts:
        oFile.write(broken + '\n')

## Write a temporary CSV
hdrs = ['_eml', '_time', '_subj', '_from', '_to', ' _cc', '_dfrom', '_dto', '_dcc', '_att', '_msg']
with open('csvWriteErrors.log', 'a') as eFile:
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
