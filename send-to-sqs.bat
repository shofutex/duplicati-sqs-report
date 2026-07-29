@echo off
setlocal

REM ###############################################################################
REM send-to-sqs.bat
REM Purpose: Duplicati "run script" hook that publishes a backup job's result output to an
REM          AWS SQS queue, for use with dupReport's protocol=sqs incoming server support.
REM          Windows/AWS-CLI equivalent of send-to-sqs.py.
REM
REM Requires: AWS CLI (aws.exe) installed and configured with credentials that can send
REM           messages to the target queue (e.g. via "aws configure", an EC2/IAM role, or the
REM           AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables). This script does
REM           not read credentials from sqs.ini; it relies on however the AWS CLI is configured
REM           on this machine.
REM
REM Config:   Set SQS_QUEUE_URL below, or set it as a persistent environment variable, so the
REM           same script can be reused across hosts without editing it. The queue must be a
REM           FIFO queue (name ends in .fifo), since this script uses
REM           --message-group-id/--message-deduplication-id, which standard queues reject.
REM
REM Configure this as Duplicati's --run-script-after=<path to this file>.
REM
REM All output from the actual work below is redirected to %TMP%\send-to-sqs-debug.txt and
REM the script always exits 0, so a failed SQS publish is never logged as a Duplicati backup
REM warning. Check that file for troubleshooting if messages aren't showing up in dupReport.
REM ###############################################################################


REM ###############################################################################
REM How to run scripts before or after backups
REM ###############################################################################

REM Duplicati is able to run scripts before and after backups. This
REM functionality is available in the advanced options of any backup job (UI) or
REM as option (CLI). The (advanced) options to run scripts are
REM --run-script-before = <filename>
REM --run-script-before-required = <filename>
REM --run-script-timeout = <time>
REM --run-script-after = <filename>
REM
REM --run-script-after = <filename>
REM Duplicati will run the script after the backup job and wait for its
REM completion for 60 seconds (default timeout value). After a timeout a
REM warning is logged. A non-zero exit code from the script is also logged as a
REM warning, but never fails or reverts the (already completed) backup job.


REM ###############################################################################
REM Special Environment Variables
REM ###############################################################################

REM DUPLICATI__OPERATIONNAME
REM Operation name can be any of the operations that Duplicati supports. For
REM example it can be "Backup", "Cleanup", "Restore", or "DeleteAllButN".

REM DUPLICATI__RESULTFILE
REM If invoked as --run-script-after this will contain the name of the
REM file where result data is placed.

REM DUPLICATI__backup_name / DUPLICATI__backup_id
REM Name/ID of the backup job, as configured with --backup-name.


REM ###############################################################################
REM Entry point - run the actual work in :main, with all its output captured to a
REM debug log instead of surfaced to Duplicati. Always report success afterward.
REM ###############################################################################

call :main > "%TMP%\send-to-sqs-debug.txt" 2>&1
exit /B 0


:main

REM Full URL of the target FIFO queue. Override by setting SQS_QUEUE_URL in the environment
REM instead of editing the default below.
IF NOT DEFINED SQS_QUEUE_URL SET SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/DuplicatiQueue.fifo

REM Only backup jobs produce a report worth sending. Other operations (e.g. Delete, Repair) are ignored.
IF NOT "%DUPLICATI__OPERATIONNAME%" == "Backup" GOTO :eof

IF NOT DEFINED DUPLICATI__RESULTFILE GOTO :eof
IF NOT EXIST "%DUPLICATI__RESULTFILE%" GOTO :eof

REM Locate aws.exe: prefer PATH, fall back to the default AWS CLI v2 install location.
where aws.exe >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    SET AWSCMD=aws.exe
) ELSE IF EXIST "%ProgramFiles%\Amazon\AWSCLIV2\aws.exe" (
    SET "AWSCMD=%ProgramFiles%\Amazon\AWSCLIV2\aws.exe"
) ELSE (
    echo send-to-sqs.bat: aws.exe not found in PATH or default install location. 1>&2
    GOTO :eof
)

REM Backup name, with a fallback if --backup-name wasn't set.
REM Note: must check DEFINED on the source variables directly - cmd.exe leaves a %VAR%
REM reference to an undefined variable as literal text rather than expanding it to empty,
REM so checking definedness only works before the value has been copied elsewhere.
IF DEFINED DUPLICATI__backup_name (
    SET "BACKUPNAME=%DUPLICATI__backup_name%"
) ELSE IF DEFINED DUPLICATI__backup_id (
    SET "BACKUPNAME=%DUPLICATI__backup_id%"
) ELSE (
    SET "BACKUPNAME=Unknown"
)
REM Strip double quotes so the backup name can't break the JSON payload below.
SET BACKUPNAME=%BACKUPNAME:"=%

SET tmpFileName="%TMP%\bat~%RANDOM%.tmp"
powershell -NoProfile -Command "[guid]::NewGuid().ToString()" > %tmpFileName%
set /p messageid=< %tmpFileName%
del %tmpFileName%

for /f "delims=" %%a in ('
    powershell -NoProfile -Command "Get-Date -format 'ddd, dd MMM yyyy HH:mm:ss zz00'"
') do set "datetime=%%a"

SET attrFileName="%TMP%\bat~%RANDOM%.tmp"
echo {"subject":{"StringValue":"Duplicati Backup report for %BACKUPNAME%","DataType":"String"},"date":{"StringValue":"%datetime%","DataType":"String"}} > %attrFileName%

"%AWSCMD%" sqs send-message --queue-url "%SQS_QUEUE_URL%" --message-body "file://%DUPLICATI__RESULTFILE%" --message-deduplication-id "%messageid%" --message-group-id "DuplicatiReports" --message-attributes "file://%attrFileName%"
IF %ERRORLEVEL% NEQ 0 echo send-to-sqs.bat: aws sqs send-message failed with error %ERRORLEVEL% 1>&2

del %attrFileName%

GOTO :eof
