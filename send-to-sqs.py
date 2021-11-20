#!/usr/bin/python3

import configparser
import os
import sys
import uuid
import email.utils
import boto3

if __name__ == "__main__":
    
    path = os.path.dirname(os.path.realpath(sys.argv[0]))

    config = configparser.ConfigParser()
    config.read(path+'/sqs.ini')

    EVENTNAME=os.environ['DUPLICATI__EVENTNAME']
    OPERATIONNAME=os.environ['DUPLICATI__OPERATIONNAME']
    REMOTEURL=os.environ['DUPLICATI__REMOTEURL']
    LOCALPATH=os.environ['DUPLICATI__LOCALPATH']

    if(OPERATIONNAME == "Backup"):
        with open(os.environ['DUPLICATI__RESULTFILE']) as resultfile:
            body = ""
            for line in resultfile:
                body = body + line

            myclient = boto3.client('sqs',
                aws_access_key_id=config['AWS Credentials']['aws-key'],
                aws_secret_access_key=config['AWS Credentials']['aws-secret-key'],
                region_name='us-east-1')

            url = config['AWS Credentials']['sqs-queue']

            myclient.send_message(QueueUrl=url,
                MessageBody=body,
                MessageGroupId='DuplicatiReports',
                MessageDeduplicationId=str(uuid.uuid4()),
                MessageAttributes={
                    'subject' : {
                        'StringValue' : "Duplicati Backup report for "+os.environ['DUPLICATI__backup_name'],
                        'DataType' : 'String'
                    },
                    'date' : {
                        'StringValue' : email.utils.formatdate(),
                        'DataType' : 'String'
                    }
                }
            )
