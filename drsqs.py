#####
#
# Module name:  drsqs
# Purpose:      Manage SQS connections for dupReport
# 
# Notes:
#
#####

# Import system modules
import quopri
import base64
import re
import datetime
import time
import os
import json
import boto3
import botocore
import email

# Import dupReport modules
import globs
import drdatetime
import dremail
from dremail import lineParts

# To test
# globs.log = log.LogHandler()
# globs.tmpLogPath='/tmp'

class SqsServer(dremail.EmailServer, object):
    def __init__(self, prot, add, prt, acct, pwd, crypt, kalive, fold = None):
        super(SqsServer,self).__init__(prot, add, prt, acct, pwd, crypt, kalive, fold)
        self.newEmails = []

    def connect(self):
        globs.log.write(1, 'SqsServer.Connect()')
        globs.log.write(3, 'server={} keepalive={}'.format(self.server, self.keepalive))

        # See if a server connection is already established
        # This is the most common case, so check this first
        if self.server != None:
            if self.keepalive is False: # Do we care about keepalives?
                return None
        else:     # Need to establish server connection
            if self.protocol == 'sqs':
                globs.log.write(1,'Initial connect using  SQS')
                try:
                    self.server = boto3.client(self.protocol, 
                            aws_access_key_id=self.accountname,
                            aws_secret_access_key=self.passwd,
                            region_name='us-east-1')

                    globs.log.write(3,'SQS Logged in.')
                    return None
                except Exception:
                    return None
            else:   # Bad protocol specification
                globs.log.err('Invalid protocol specification: {}. Aborting program.'.format(self.protocol))
                globs.closeEverythingAndExit(1)
                return None
        return None


    # Check if there are new messages waiting on the server
    # Return number of messages if there
    # Return None if empty
    def checkForMessages(self):
        self.connect()
        if self.protocol == 'sqs':
            globs.log.write(1,'checkForMessages(SQS)')
            queue = self.server.get_queue_url(QueueName=self.address)['QueueUrl']

            messagesRemaining = True
            while messagesRemaining:
                thesemessages = self.server.receive_message(QueueUrl=queue,AttributeNames=['All'],MessageAttributeNames=['subject','date'])
                if 'Messages' in thesemessages:
                    for msg in thesemessages['Messages']:
                        self.newEmails.append(msg)
                        self.server.delete_message(QueueUrl=queue,ReceiptHandle=msg['ReceiptHandle'])
                else:
                    messagesRemaining = False

            self.numEmails = len(self.newEmails)
            self.nextEmail = -1
            return self.numEmails

        else:  # Invalid protocol
            return None


    # Retrieve and process next message from server
    # Returns <Message-ID> or '<INVALID>' if there are more messages in queue, even if this message was unusable
    # Returns None if no more messages
    def processNextMessage(self):
        globs.log.write(1, 'drsqs.processNextMessage()')
        self.connect()

        # Increment message counter to the next message. 
        # Skip for message #0 because we haven't read any messages yet
        self.nextEmail += 1

        msgParts = {}       # msgParts contains extracts of message elements
        statusParts = {}    # statusParts contains the individual lines from the Duplicati status emails
        dateParts = {}      # dateParts contains the date & time strings for the SQL Query

        # Check no-more-mail conditions. Either no new emails to get or gone past the last email on list
        if (len(self.newEmails) == 0):  
            return None

        if self.protocol == 'sqs':
            msg = self.newEmails.pop(0)
            msgBody = msg['Body']

            msgParts['date'] = msg['MessageAttributes']['date']['StringValue']
            msgParts['subject'] = msg['MessageAttributes']['subject']['StringValue']
            msgParts['messageId'] = msg['MD5OfBody']
            msgParts['content-transfer-encoding'] = '7bit'

        else:   # Invalid protocol spec
            globs.log.err('Invalid protocol specification: {}.'.format(self.protocol))
            return None
            
        # Log message basics
        globs.log.write(1,'\n*****\nNext Message: Date=[{}] Subject=[{}] Message-Id=[{}] Transfer-Encoding=[{}]'.format(msgParts['date'], msgParts['subject'], msgParts['messageId'], msgParts['content-transfer-encoding']))
        
        # Check if any of the vital parts are missing
        if msgParts['messageId'] is None or msgParts['messageId'] == '':
            globs.log.write(1,'No message-Id. Abandoning processNextMessage()')
            return '<INVALID>'
        if msgParts['date'] is None or msgParts['date'] == '':
            globs.log.write(1,'No Date. Abandoning processNextMessage()')
            return msgParts['messageId']
        if msgParts['subject'] is None or msgParts['subject'] == '':
            globs.log.write(1,'No Subject. Abandoning processNextMessage()')
            return msgParts['messageId']

        # See if it's a message of interest
        # Match subject field against 'subjectregex' parameter from RC file (Default: 'Duplicati Backup report for...')
        if re.search(globs.opts['subjectregex'], msgParts['subject']) == None:
            globs.log.write(1, 'Message [{}] is not a Message of Interest. Can\'t match subjectregex from .rc file. Skipping message.'.format(msgParts['messageId']))
            return msgParts['messageId']    # Not a message of Interest

        # Get source & desination computers from email subject
        srcRegex = '{}{}'.format(globs.opts['srcregex'], re.escape(globs.opts['srcdestdelimiter']))
        destRegex = '{}{}'.format(re.escape(globs.opts['srcdestdelimiter']), globs.opts['destregex'])
        globs.log.write(3,'srcregex=[{}]  destRegex=[{}]'.format(srcRegex, destRegex))
        partsSrc = re.search(srcRegex, msgParts['subject'])
        partsDest = re.search(destRegex, msgParts['subject'])
        if (partsSrc is None) or (partsDest is None):    # Correct subject but delimeter not found. Something is wrong.
            globs.log.write(2,'SrcDestDelimeter [{}] not found in subject line. Skipping message.'.format(globs.opts['srcdestdelimiter']))
            return msgParts['messageId']

        # See if the record is already in the database, meaning we've seen it before
        if globs.db.searchForMessage(msgParts['messageId']):    # Is message is already in database?
            # Mark the email as being seen in the database
            globs.db.execSqlStmt('UPDATE emails SET dbSeen = 1 WHERE messageId = \"{}\"'.format(msgParts['messageId']))
            globs.db.dbCommit()
            return msgParts['messageId']
        # Message not yet in database. Proceed.
        globs.log.write(1, 'Message ID [{}] does not yet exist in DB.'.format(msgParts['messageId']))

        dTup = email.utils.parsedate_tz(msgParts['date'])
        if dTup:
            # See if there's timezone info in the email header data. May be 'None' if no TZ info in the date line
            # TZ info is represented by seconds offset from UTC
            # We don't need to adjust the email date for TimeZone info now, since date line in email already accounts for TZ.
            # All other calls to toTimestamp() should include timezone info
            msgParts['timezone'] = dTup[9]

            # Set date into a parseable string
            # It doesn't matter what date/time format we pass in (as long as it's valid)
            # When it comes back out later, it'll be parsed into the user-defined format from the .rc file
            # For now, we'll use YYYY/MM/DD HH:MM:SS
            xDate = '{:04d}/{:02d}/{:02d} {:02d}:{:02d}:{:02d}'.format(dTup[0], dTup[1], dTup[2], dTup[3], dTup[4], dTup[5])  
            dtTimStmp = drdatetime.toTimestamp(xDate, dfmt='YYYY/MM/DD', tfmt='HH:MM:SS')  # Convert the string into a timestamp
            msgParts['emailTimestamp'] = dtTimStmp
            globs.log.write(3, 'emailDate=[{}]-[{}]'.format(dtTimStmp, drdatetime.fromTimestamp(dtTimStmp)))

        msgParts['sourceComp'] = re.search(srcRegex, msgParts['subject']).group().split(globs.opts['srcdestdelimiter'])[0]
        msgParts['destComp'] = re.search(destRegex, msgParts['subject']).group().split(globs.opts['srcdestdelimiter'])[1]
        globs.log.write(3, 'sourceComp=[{}] destComp=[{}] emailTimestamp=[{}] subject=[{}]'.format(msgParts['sourceComp'], \
            msgParts['destComp'], msgParts['emailTimestamp'], msgParts['subject']))

        # Search for source/destination pair in database. Add if not already there
        retVal = globs.db.searchSrcDestPair(msgParts['sourceComp'], msgParts['destComp'])

        globs.log.write(3, 'Message Body=[{}]'.format(msgBody))

        if msgParts['content-transfer-encoding'].lower() == 'quoted-printable':
            msgBody = quopri.decodestring(msgBody.replace('=0D=0A','\n')).decode("utf-8")
            globs.log.write(3, 'New (quopri) Message Body=[{}]'.format(msgBody))

        # See if email is text or JSON. JSON messages begin with '{"Data":'
        globs.log.write(3, "msgBody[:8] = [{}]".format(msgBody[:8]))
        isJson = True if msgBody[:8] == '{\"Data\":' else False

        if isJson:
            jsonStatus = json.loads(msgBody.replace("=\r\n","").replace("=\n",""), strict = False)    # Top-level JSON data
            jsonData = jsonStatus['Data']                                           # 'Data' branch under main data

            # Get message fields from JSON column in lineParts list
            for section,regex,flag,typ,jsonSection in lineParts:
                statusParts[section] = self.searchMessagePartJson(jsonData, jsonSection, typ)
            # See if there are log lines to display
            if len(jsonStatus['LogLines']) > 0:
                statusParts['logdata'] = jsonStatus['LogLines'][0]
            else:
               statusParts['logdata'] = ''

            if statusParts['parsedResult'] != 'Success': # Error during backup
                # Set appropriate fail/message fields to relevant values
                # The JSON report has somewhat different fields than the "classic" report, so we have to fudge this a little bit
                #   so we can use common code to process both types later.
                statusParts['failed'] = 'Failure'   
                if statusParts['parsedResult'] == '':
                    statusParts['parsedResult'] = 'Failure'   
                statusParts['errors'] = jsonData['Message'] if 'Message' in jsonData else ''
        else: # Not JSON - standard message format
            # Go through each element in lineParts{}, get the value from the body, and assign it to the corresponding element in statusParts{}
            for section,regex,flag,typ, jsonSection in lineParts:
                statusParts[section] = self.searchMessagePart(msgBody, regex, flag, typ) # Get the field parts

        # Adjust fields if not a clean run
        globs.log.write(3, "statusParts['failed']=[{}]".format(statusParts['failed']))
        if statusParts['failed'] == '':  # Looks like a good run
            # Get the start and end times of the backup
            if  isJson:
                dateParts['endTimestamp'] = drdatetime.toTimestampRfc3339(statusParts['endTimeStr'], utcOffset = msgParts['timezone'])
                dateParts['beginTimestamp'] = drdatetime.toTimestampRfc3339(statusParts['beginTimeStr'], utcOffset = msgParts['timezone'])
            else:
                # Some fields in "classic" Duplicati report output are displayed in standard format or detailed format (in parentheses)
                # For example:
                #   SizeOfModifiedFiles: 23 KB (23556)
                #   SizeOfAddedFiles: 10.12 KB (10364)
                #   SizeOfExaminedFiles: 44.42 GB (47695243956)
                #   SizeOfOpenedFiles: 33.16 KB (33954)
                # JSON output format does not use parenthesized format (see https://forum.duplicati.com/t/difference-in-json-vs-text-output/7092 for more explanation)

                # Extract the parenthesized value (if present) or the raw value (if not)
                dt, tm = globs.optionManager.getRcSectionDateTimeFmt(msgParts['sourceComp'], msgParts['destComp'])
                dateParts['endTimestamp'] = self.parenOrRaw(statusParts['endTimeStr'], df = dt, tf = tm, tz = msgParts['timezone'])
                dateParts['beginTimestamp'] = self.parenOrRaw(statusParts['beginTimeStr'], df = dt, tf = tm, tz = msgParts['timezone'])
                statusParts['sizeOfModifiedFiles'] = self.parenOrRaw(statusParts['sizeOfModifiedFiles'])
                statusParts['sizeOfAddedFiles'] = self.parenOrRaw(statusParts['sizeOfAddedFiles'])
                statusParts['sizeOfExaminedFiles'] = self.parenOrRaw(statusParts['sizeOfExaminedFiles'])
                statusParts['sizeOfOpenedFiles'] = self.parenOrRaw(statusParts['sizeOfOpenedFiles'])

            globs.log.write(3, 'Email indicates a successful backup. Date/time is: end=[{}]  begin=[{}]'.format(dateParts['endTimestamp'], dateParts['beginTimestamp'])), 
        else:  # Something went wrong. Let's gather the details.
            if not isJson:
                statusParts['errors'] = statusParts['failed']
                statusParts['parsedResult'] = 'Failure'
                statusParts['warnings'] = statusParts['details']

            globs.log.write(2, 'Errors=[{}]'.format(statusParts['errors']))
            globs.log.write(2, 'Warnings=[{}]'.format(statusParts['warnings']))
            globs.log.write(2, 'Log Data=[{}]'.format(statusParts['logdata']))

            # Since the backup job report never ran, we'll use the email date/time as the report date/time
            dateParts['endTimestamp'] = msgParts['emailTimestamp']
            dateParts['beginTimestamp'] = msgParts['emailTimestamp']
            globs.log.write(3, 'Email indicates a failed backup. Replacing date/time with: end=[{}]  begin=[{}]'.format(dateParts['endTimestamp'], dateParts['beginTimestamp'])), 

        # Replace commas (,) with newlines (\n) in message fields. Sqlite really doesn't like commas in SQL statements!
        for part in ['messages', 'warnings', 'errors', 'logdata']:
            if statusParts[part] != '':
                statusParts[part] = statusParts[part].replace(',','\n')

        # If we're just collecting and get a warning/error, we may need to send an email to the admin
        if (globs.opts['collect'] is True) and (globs.opts['warnoncollect'] is True) and ((statusParts['warnings'] != '') or (statusParts['errors'] != '')):
            errMsg = 'Duplicati error(s) on backup job\n'
            errMsg += 'Message ID {} on {}\n'.format(msgParts['messageId'], msgParts['date'])
            errMsg += 'Subject: {}\n\n'.format(msgParts['subject'])
            if statusParts['warnings'] != '':
                errMsg += 'Warnings:' + statusParts['warnings'] + '\n\n'
            if statusParts['errors'] != '':
                errMsg += 'Errors:' + statusParts['errors'] + '\n\n'
            if statusParts['logdata'] != '':
                errMsg += 'Log Data:' + statusParts['logdata'] + '\n\n'

            globs.outServer.sendErrorEmail(errMsg)

        globs.log.write(3, 'Resulting timestamps: endTimeStamp=[{}] beginTimeStamp=[{}]'.format(drdatetime.fromTimestamp(dateParts['endTimestamp']), drdatetime.fromTimestamp(dateParts['beginTimestamp'])))

        globs.db.execEmailInsertSql(msgParts, statusParts, dateParts)
        return msgParts['messageId']


    # Issue #111 feature request
    # Provide ability to mark messages as read/seen if [main]optread is true in the .rc file.
    # This function is only works for IMAP. POP3 doesn't have this capability.
    # Nor does SQS
    def markMessagesRead(self):
        return;

