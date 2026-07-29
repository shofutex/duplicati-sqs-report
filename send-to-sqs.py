#!/usr/bin/env python3

#####
#
# Program name: send-to-sqs.py
# Purpose:      Duplicati "run script" hook that publishes a backup job's result output to an AWS SQS
#               queue instead of (or in addition to) emailing it. dupReport can then be configured with
#               an [incoming] server section using protocol=sqs to read job results from that queue.
#
# Usage:        Configure Duplicati to run this script after a backup job completes
#               (--run-script-after=/path/to/send-to-sqs.py), with --run-script-timeout as needed.
#               Duplicati must also be configured to write job results to a file (--result-file-format
#               and a --run-script-before/after ResultFile as documented by Duplicati) so that
#               DUPLICATI__RESULTFILE points at the report to send.
#
# Config:       Reads AWS credentials & queue info from sqs.ini (see sqs.ini.EXAMPLE), located in the
#               same directory as this script.
#
#####

import configparser
import os
import sys
import uuid
import email.utils
import boto3

if __name__ == "__main__":

    path = os.path.dirname(os.path.realpath(sys.argv[0]))

    config = configparser.ConfigParser()
    config.read(os.path.join(path, 'sqs.ini'))

    operationName = os.environ.get('DUPLICATI__OPERATIONNAME', '')

    # Only backup jobs produce a report worth sending. Other operations (e.g. Delete, Repair) are ignored.
    if operationName == 'Backup':
        resultFile = os.environ['DUPLICATI__RESULTFILE']
        with open(resultFile) as f:
            body = f.read()

        backupName = os.environ.get('DUPLICATI__backup_name', os.environ.get('DUPLICATI__backup_id', 'Unknown'))

        client = boto3.client('sqs',
            aws_access_key_id=config['AWS Credentials']['aws-key'],
            aws_secret_access_key=config['AWS Credentials']['aws-secret-key'],
            region_name=config['AWS Credentials'].get('region', 'us-east-1'))

        queueUrl = config['AWS Credentials']['sqs-queue']

        client.send_message(QueueUrl=queueUrl,
            MessageBody=body,
            MessageGroupId='DuplicatiReports',
            MessageDeduplicationId=str(uuid.uuid4()),
            MessageAttributes={
                'subject': {
                    'StringValue': 'Duplicati Backup report for ' + backupName,
                    'DataType': 'String'
                },
                'date': {
                    'StringValue': email.utils.formatdate(),
                    'DataType': 'String'
                }
            }
        )
