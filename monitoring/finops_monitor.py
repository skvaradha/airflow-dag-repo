import json
import os
import boto3
import datetime
import logging

MONITORING_BUCKET = os.environ.get('FINOPS_MONITORING_BUCKET')
HEARTBEAT_MONITORING_BUCKET = os.environ.get('FINOPS_HEARTBEAT_BUCKET')
SEVERITY = ['critical', 'high', 'medium', 'low'] # Severity list used for matching DAG level tags

def dump_json_to_s3(bucket: str, key: str, message: dict) -> None:
    """
    This function is used to write a python dict object to a S3 destination.
    :param bucket: S3 bucket under which you want to store the file
    :param key: File keu for storage
    :param message: The python dict you want to push as a file onto S3
    :return: None
    """
    s3_resource = boto3.resource('s3', region_name='us-east-1')
    s3_resource.Bucket(bucket).put_object(Key=key, Body=json.dumps(message, default=str))

def finops_monitor(context) -> None:
    """
    This function will be used as the on failure callback to write a JSON file to finops-monitor bucket.
    File naming convention : <dag_name>.<timestamp>.json
    :param context: It is the airflow context passed down from the task
    :return: None
    """
    logging.info("Reporting to monitoring bucket")
    task_id = context.get('task_instance').task_id
    dag = context.get('task_instance').dag_id
    dag_no_chunk = dag.split('_')[0]
    exception = context.get('exception')
    exec_date = context.get('ts_nodash')
    org_id = task_id.split('-')[0] if task_id.split('-')[0]!='' else 'noorg'
    tags = context["dag"].tags
    failure_severity = [x for x in tags if x.lower() in SEVERITY][0] # Matches the tags from DAG to find the severity
    severity = failure_severity if failure_severity else 'Critical' # If no matches, defaulting to Critical

    message = {"org_id": org_id,
               "resource_id": 'sc-customer-'+org_id,
               "account_id": 'act-'+org_id,
               "level": 'Error',
               "channel": dag_no_chunk,
               "message": "Org ID : "+org_id+ ", Account ID : act-"+org_id + ", Exception: "+ str(exception),
               "severity": severity,
               "customFieldsOpen": '',
               "createdAt": datetime.datetime.now()}

    key = dag_no_chunk + "/" + org_id + "." + exec_date +".json"
    dump_json_to_s3(MONITORING_BUCKET, key, message)


def finops_heartbeat_monitor(context) -> None:
    """
    This function will be used as the on success callback to write a heartbeat JSON file to finops-monitor-heartbeat bucket.
    File naming convention : <dag_name>.<timestamp>.json
    :param context: It is the airflow context passed down from the task
    :return: None
    """
    logging.info("Reporting to heartbeat bucket")
    task_id = context.get('task_instance').task_id
    dag = context.get('task_instance').dag_id
    dag_no_chunk = dag.split('_')[0]
    exec_date = context.get('ts_nodash')
    org_id = task_id.split('-')[0] if task_id.split('-')[0] != '' else 'noorg'

    message = {"org_id": org_id,
               "resource_id": 'sc-customer-'+org_id,
               "account_id": 'act-'+org_id,
               "level": 'Heartbeat',
               "channel": dag_no_chunk,
               "message": "Org ID : "+org_id+ ", Account ID : act-"+org_id + " ingestion successfully completed",
               "createdAt": datetime.datetime.now()}
    key = dag_no_chunk + "/" + org_id + "." + exec_date +".json"
    dump_json_to_s3(HEARTBEAT_MONITORING_BUCKET, key, message)