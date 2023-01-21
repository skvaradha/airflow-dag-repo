from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.utils.trigger_rule import TriggerRule
from finops.billing.models.enums.report_formatter import ReportFormatter
from finops.billing.models.report import Report
from finops.billing.report.athena_queries import AthenaQueryComposer
import boto3
import datetime
import logging
import os
import pandas as pd
import uuid

# Create random string
u = str(uuid.uuid4())
random_str = u.split('-')[0]


# This class is used to execute athena query in AWS
class CustomAWSAthenaOperator(AWSAthenaOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        report_id = 1
        organization_id = context['ti'].xcom_pull(task_ids='get_params', key='organization_id')
        customer = context['ti'].xcom_pull(task_ids='get_params', key='name')
        temp_table_name = context['ti'].xcom_pull(task_ids='get_params',
                                                  key='temp_table_name')
        query_composer = AthenaQueryComposer(report_id=report_id,
                                             organization_id=organization_id,
                                             customer=customer,
                                             table_name=temp_table_name)

        if context['task'].task_id == 'create_temp_table':
            modified_cur_location = context['ti'].xcom_pull(task_ids='get_params',
                                                            key='modified_cur_location')
            self.query = query_composer.get_create_table_query(modified_cur_location=modified_cur_location)
        if context['task'].task_id == 'delete_temp_table':
            self.query = query_composer.get_delete_table_query()

        logging.info('Athena query: {}'.format(self.query))
        super().execute(context)

        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id


# Create and delete temp table
def build_athena_query_step(task_id, dag):
    athena_result_location = 's3://{}/temp/'.format(os.getenv('MSP_BUCKET', 'msp-audit'))
    athena_db = 'temp_tables'

    # the query will be resolved dynamically inside CustomAWSAthenaOperator's execute() function based on the task_id
    return CustomAWSAthenaOperator(
        task_id=task_id,
        query='',
        output_location=athena_result_location,
        database=athena_db,
        aws_conn_id='aws_default',
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED,
        retries=3
    )


# Move files from temp folder to report folder
def get_move_results_step(generate_report_task_id, task_id, dag, customer):
    if generate_report_task_id == 'get_report_query_prev_month':
        file_name = "{{ task_instance.xcom_pull(key='return_value', task_ids='get_report_query_prev_month') }}"
        output = "{{ task_instance.xcom_pull(key='prev_report_output', task_ids='get_params') }}"
    elif generate_report_task_id == 'get_report_query_this_month':
        file_name = "{{ task_instance.xcom_pull(key='return_value', task_ids='get_report_query_this_month') }}"
        output = "{{ task_instance.xcom_pull(key='report_output', task_ids='get_params') }}"

    return S3FileTransformOperator(
        task_id=task_id,
        source_s3_key=f"s3://{os.getenv('MSP_BUCKET', 'msp-audit')}/temp/{file_name}.csv",
        dest_s3_key=output,
        replace=True,
        transform_script='/bin/cp',
        trigger_rule='one_success',
        dag=dag
    )


# Move files from temp folder to report folder
def archive_step(task_id, dag):
    if task_id == 'archive_prev_month_file':
        file_path = "{{ task_instance.xcom_pull(key='prev_report_output', task_ids='get_params') }}"
        output = "{{ task_instance.xcom_pull(key='archive_prev_month_file', task_ids='get_params') }}"
    if task_id == 'archive_this_month_file':
        file_path = "{{ task_instance.xcom_pull(key='report_output', task_ids='get_params') }}"
        output = "{{ task_instance.xcom_pull(key='archive_this_month_file', task_ids='get_params') }}"
    return S3FileTransformOperator(
        task_id=task_id,
        source_s3_key=file_path,
        dest_s3_key=output,
        replace=True,
        transform_script='/bin/cp',
        trigger_rule='one_success',
        dag=dag
    )


# Get items details. Need to be replaced by api call
# def fetch_api_data() -> List[CustomReport]:
#     customer_client = CustomerServiceClient(os.getenv("CUSTOMER_SERVICE_URL"), provider='aws')
#     custom_report = customer_client.get().custom_reports()
#     return custom_report


# Create generate report query
def create_report(customer, firstday, today, task_id, dag):
    print(firstday, today)
    athena_result_location = 's3://{}/temp/'.format(os.getenv('MSP_BUCKET', 'msp-audit'))
    athena_db = 'temp_tables'

    report_dict = {
        'id': 1,
        'organization_id': customer.organization_id,
        'name': customer.name,
        'start_date': firstday,
        'end_date': today,
        'workflow_id': customer.workflow_id,
        'subset_ids': customer.subset_ids,
        'aggregations': customer.aggregations
    }
    report = Report(**report_dict)
    print(report.__dict__)

    temp_table = "{{ task_instance.xcom_pull(key='temp_table_name', task_ids='get_params') }}"
    query = ReportFormatter(report, customer.organization_id).generate_report_query(temp_table, customer.name)

    # the query will be resolved dynamically inside CustomAWSAthenaOperator's execute() function based on the task_id
    return CustomAWSAthenaOperator(
        task_id=task_id,
        query=query,
        output_location=athena_result_location,
        database=athena_db,
        aws_conn_id='aws_default',
        dag=dag,
        trigger_rule='one_success',
        retries=3
    )


# Create year month pair for EMR
def create_yearmonth_param():
    today_date = datetime.datetime.today()
    first_date = today_date.replace(day=1)
    prev_month_date = first_date - datetime.timedelta(days=1)

    if today_date.day <= 15:
        year_month_pair = f"{prev_month_date.year}_{prev_month_date.month} {today_date.year}_{today_date.month}"
    else:
        year_month_pair = f"{today_date.year}_{today_date.month}"
    return year_month_pair


# set XCOM values for EMR
def push_xcom(customer, **context):
    year_month_pair = create_yearmonth_param()
    now = datetime.datetime.now()
    first_day_of_month = now.replace(day=1)
    previous_month_last_day = (first_day_of_month - datetime.timedelta(days=1))
    report_name = f"SPOT_MSP_REPORT_{customer.name}_{now.year}_{now.month}"
    prev_report_name = f"SPOT_MSP_REPORT_{customer.name}_{previous_month_last_day.year}_{previous_month_last_day.month}"
    report_output = f'{customer.s3_location}/{report_name}.csv'
    prev_report_output = f'{customer.s3_location}/{prev_report_name}.csv'
    archive_this_month = f"s3://{os.getenv('MSP_BUCKET', 'msp-audit')}" \
                         f"/fixed_report_run/archive/customer={customer.name}/{report_name}_{now.strftime('%Y%m%d%H%M%S')}"
    archive_prev_month = f"s3://{os.getenv('MSP_BUCKET', 'msp-audit')}" \
                         f"/fixed_report_run/archive/customer={customer.name}/{prev_report_name}_{now.strftime('%Y%m%d%H%M%S')}"
    context['ti'].xcom_push(key='customer', value=customer.name)
    context['ti'].xcom_push(key='years_months_pairs', value=year_month_pair)
    context['ti'].xcom_push(key='workflow_id', value=customer.workflow_id)
    context['ti'].xcom_push(key='subset_ids', value=customer.subset_ids)
    context['ti'].xcom_push(key='organization_id', value=customer.organization_id)
    context['ti'].xcom_push(key='report_output', value=report_output)
    context['ti'].xcom_push(key='prev_report_output', value=prev_report_output)
    context['ti'].xcom_push(key='temp_table_name',
                            value=f'report_modified_cur_{random_str}')
    context['ti'].xcom_push(key='modified_cur_location',
                            value=f"s3://{os.getenv('MSP_BUCKET', 'msp-audit')}"
                                  f"/fixed_report_run/customer={customer.name}/workflow_id={customer.workflow_id}/abcd")
    context['ti'].xcom_push(key='archive_this_month_file', value=archive_this_month)
    context['ti'].xcom_push(key='archive_prev_month_file', value=archive_prev_month)


# Function for branching
def identify_month_run(day, one_month_run, two_month_run):
    if day <= 15:
        return two_month_run
    else:
        return one_month_run


def archive_audit_files(**context):
    # create path variable to archive audit file
    # archive_path = "{{ task_instance.xcom_pull(key='archive_this_month_file', task_ids='get_params') }}"
    archive_path = context['ti'].xcom_pull(task_ids='get_params', key='archive_this_month_file')
    final_archive_path = "/".join(archive_path.split("/", 6)[:6])
    final_archive_path = f"{final_archive_path}/audit_log_{datetime.datetime.now().strftime('%Y%m%m%H%M%S')}"

    # read audit log into pandas and create a single audit file for archiving
    # file = "{{ task_instance.xcom_pull(key='modified_cur_location', task_ids='get_params') }}"
    file = context['ti'].xcom_pull(task_ids='get_params', key='modified_cur_location')
    bucket_name = file.split("/", 3)[2]

    prefix = "/".join(file.split("/", 3)[3:])
    prefix = f'{prefix}-audit_log/'

    # do a boto3 call for s3 operation
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    df = pd.DataFrame()
    for object_summary in my_bucket.objects.filter(
            Prefix=prefix):
        if object_summary.key[-4:] == '.csv':
            print(object_summary.key)
            df_temp = pd.read_csv(f"s3://msp-audit-prod/{object_summary.key}")
            df = pd.concat([df, df_temp], axis=0)

    # write to archive path
    df.to_csv(final_archive_path, index=False)
