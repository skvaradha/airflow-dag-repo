import os
import boto3
import botocore
import json
import time
import uuid
from collections import OrderedDict
from io import BytesIO
from gzip import GzipFile
import logging
import traceback
import sys

EVENT_LOGS_BUCKET = os.getenv('LOGGING_BUCKET')
s3_resouce = boto3.resource('s3', region_name='us-east-1')


def log_event(event_type, event_source, output_capture=[], **kwargs):
    # Default data
    modules = []
    modules.append({'name': 'JSON', 'version': str(json.__version__)})
    modules.append({'name': 'boto3', 'version': str(boto3.__version__)})
    modules.append({'name': 'botocore', 'version': str(botocore.__version__)})
    modules.append({'name': 'python', 'version': sys.version})
    log_id = str(uuid.uuid4())
    event_version = '1.0'
    # end default
    log_data = dict()

    log_data['eventVersion'] = event_version
    log_data['dataValidation'] = 'Valid'
    log_data['dataValidationInfo'] = []

    log_data['userIdentity'] = OrderedDict()
    log_data['userIdentity']['type'] = 'IAMKey'
    log_data['userIdentity']['principalId'] = 'accesskey!!'
    log_data['userIdentity']['arn'] = 'arn here!'
    log_data['userIdentity']['accountId'] = ''
    log_data['userIdentity']['userName'] = ''
    log_data['customer'] = OrderedDict()
    log_data['customer']['name'] = ''

    log_data['eventTime'] = (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    # log_data['event_source'] = event_source
    log_data['awsRegion'] = 'region here'

    log_data['requestId'] = ''
    log_data['eventId'] = log_id
    log_data['eventDesc'] = ''
    log_data['event_type'] = event_type  # 'error|warning|notice|log'

    log_data['app'] = OrderedDict()
    log_data['app']['environment'] = ''
    log_data['app']['resourceId'] = ''  # instance id, etc
    log_data['app']['name'] = ''
    log_data['app']['codeVersion'] = ''
    log_data['app']['platform'] = sys.platform
    log_data['app']['arguments'] = sys.argv
    log_data['app']['paths'] = sys.path
    log_data['app']['callStack'] = traceback.format_exc().splitlines()

    log_data['app']['modules'] = modules

    log_data['app']['msgs'] = []
    log_data['time_elapsed'] = 0

    # Standard debugging info
    msg = OrderedDict()
    msg['type'] = 'traceback'
    msg['timestamp'] = (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    msg['data'] = traceback.format_exc().splitlines()
    log_data['app']['msgs'].append(msg)

    if len(output_capture) > 0:
        # if any output was included debugging info
        msg = OrderedDict()
        msg['type'] = 'outputCapture'
        msg['timestamp'] = (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        msg['data'] = output_capture.splitlines()
        log_data['app']['msgs'].append(msg)

    for key, value in kwargs['log_info'].items():
        # print('{0} = {1}'.format(key, value))
        # print type(kwargs['log_info'][key])

        if key in log_data:
            if type(kwargs['log_info'][key]) == type(dict()):

                # 2nd layer
                for key2, value2 in kwargs['log_info'][key].items():
                    if key2 in log_data[key]:
                        if type(kwargs['log_info'][key][key2]) == type(dict()):

                            # 3rd layer
                            for key3, value3 in kwargs['log_info'][key][key2].items():
                                if key3 in log_data[key][key2]:
                                    log_data[key][key2][key3] = value3
                                else:
                                    log_data['dataValidationInfo'].append(
                                        'NOT VALID PARAM: [{0}][{1}][{2}] = {3}'.format(key, key2, key3, value3))

                        if type(kwargs['log_info'][key][key2]) == type([]):

                            for x in kwargs['log_info'][key][key2]:
                                if type(x) == type(dict()):
                                    # 3rd layer
                                    temp_dict = {}
                                    for key3, value3 in x.items():

                                        found = False
                                        for y in log_data[key][key2]:
                                            if key3 in y:
                                                temp_dict[key3] = value3
                                                # log_data[key][key2].append(value3)
                                                found = True
                                        if not found:
                                            log_data['dataValidationInfo'].append(
                                                'NOT VALID PARAM: [{0}][{1}][{2}] = \'{3}\''.format(key, key2, key3,
                                                                                                    value3))
                                    log_data[key][key2].append(temp_dict)
                                else:
                                    log_data[key][key2].append(x)

                        else:
                            log_data[key][key2] = value2
                    else:
                        log_data['dataValidationInfo'].append(
                            'NOT VALID PARAM: [{0}][{1}] = {2}'.format(key, key2, value2))

            elif key in log_data:
                log_data[key] = value

            else:
                log_data['dataValidationInfo'].append('NOT VALID PARAM: {0} = {1}'.format(key, value))

    logging_JSON_flat = json.dumps(log_data)

    # Zip it up
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    gz.write(logging_JSON_flat.encode('utf-8'))
    gz.close()

    current_year = time.strftime("%Y", time.gmtime())
    current_month = time.strftime("%m", time.gmtime())

    # dump into s3 bucket
    key = "EventLogs-Data/eventSource=" + event_source + "/year=" + current_year + "/month=" + current_month + "/" + log_id + ".json.gz"
    s3_resouce.Bucket(EVENT_LOGS_BUCKET).put_object(Key=key, Body=gz_body.getvalue())

    log_format = os.getenv("LOG_FORMAT", "PRETTY")
    if log_format == "PRETTY":
        message = json.dumps(log_data, indent=4, sort_keys=True, default=str)
    else:
        message = json.dumps(log_data, default=str)

    logging.warning(message)
    return log_data
