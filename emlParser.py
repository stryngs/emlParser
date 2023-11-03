#!/usr/bin/python3
import csv
import os
import mimetypes
import shutil
import sqlite3 as lite
import time
from email import policy
from email.parser import BytesParser

## os prep
tStart = time.time()
tVal = tStart
if os.path.isfile('errParsing.log'):
    os.remove('errParsing.log')
if os.path.isfile('errAttachment.log'):
    os.remove('errAttachment.log')
if os.path.isfile('errSql.log'):
    os.remove('errSql.log')
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
        try:
            _TO = [i for i in msg.get('To').addresses]
            _to = '\n'.join([i.addr_spec for i in _TO])
            _dto = '\n'.join([i.display_name for i in _TO])
        except:
            _to = None
            _dto = None

        ## attachment parsing ideas-> https://gist.github.com/dukedougal/9301467
        try:
            hasAttach = 0
            extension = None
            aDir = 'attachments/{0}'.format(eml.split('.')[0])
            for attachment in msg.iter_attachments():

                ## Increase count
                hasAttach += 1

                ## filename and extensions
                fn = attachment.get_filename()
                if fn:
                    extension = os.path.splitext(attachment.get_filename())[1]
                else:
                    extension = mimetypes.guess_extension(attachment.get_content_type())
                    if extension is None:
                        extension = '.txt'
                if len(extension) == 0:
                    extension = '.txt'

                ## obtain the data
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
            hasAttach = 'ERR'
            with open('errAttachment.log', 'a') as oFile:
                oFile.write(f'- eml: {eml}\n')
                oFile.write(f'ERROR: {E}\n')
                oFile.write(f'aDir: {aDir}\n')
                oFile.write(f'fn: {fn}\n')
                oFile.write(f'ext: {extension}\n')
                oFile.write('dType: {0}\n\n'.format(type(data)))

        ## Update eList
        eList.append((eml, dt, subj, _from, _to, _cc, _dfrom, _dto, _dcc, hasAttach, txt, ''))

    ## Let the user know the status and update the count
    if cnt % stdCnt == 0:
        oVal = tVal
        tVal = time.time()
        print(cnt, tVal, tVal - oVal, tVal - tStart)
    cnt += 1

## Notate failed email parsing
with open('errParsing.log', 'w') as oFile:
    for broken in brokenCounts:
        oFile.write(broken + '\n')

## results to sql
# con = lite.connect('eml.sqlite3', isolation_level = None)                     ## defaulted choice for memory safe purposes
con = lite.connect('eml.sqlite3')                                               ## try not to run out of RAM
db = con.cursor()
db.execute("""
           CREATE TABLE eml(_eml TEXT,
                            _time TEXT,
                            _subj TEXT,
                            _from TEXT,
                            _to TEXT,
                            _cc TEXT,
                            _dfrom TEXT,
                            _dto TEXT,
                            _dcc TEXT,
                            _att TEXT,
                            _msg TEXT,
                            _notes TEXT);
           """)
with open('errSql.log', 'w') as eFile:
    for e in eList:
        try:
            db.execute("""
                       INSERT INTO eml VALUES(?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?,
                                              ?);
                       """, (e[0],
                             e[1],
                             e[2],
                             e[3],
                             e[4],
                             e[5],
                             e[6],
                             e[7],
                             e[8],
                             e[9],
                             e[10],
                             e[11]))
        except Exception as E:
            eFile.write(f'- {e[0]}\n')
            eFile.write(e[-2] + '\n\n\n')

## cleanup
con.commit()
con.close()
print(' Total runtime of ' + str(int(time.time() - tStart)) + ' seconds\n')
