import boto3

from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowFailException


class QuickSightIngestionSensor(BaseSensorOperator):
    def __init__(self, create_quicksight_ingestion_task_group, *args, **kwargs):
        super(QuickSightIngestionSensor, self).__init__(*args, **kwargs)
        self.boto3_qs_client = boto3.client('quicksight')
        self.create_quicksight_ingestion_task_group = create_quicksight_ingestion_task_group

    def poke(self, context):
        qs_client = self.boto3_qs_client
        qs_ingest_params = context['ti'].xcom_pull(task_ids=f"{self.create_quicksight_ingestion_task_group}.create_quicksight_ingestion", key='return_value')

        create_ingest_resp = qs_client.describe_ingestion(
            DataSetId=qs_ingest_params['data_set_id'],
            AwsAccountId=qs_ingest_params['aws_account_id'],
            IngestionId=qs_ingest_params['ingest_id']
        )

        ingest_status = create_ingest_resp['Ingestion']['IngestionStatus']
        if ingest_status == 'FAILED':
            err_msg = "Ingest for data-set:{} failed.".format(qs_ingest_params['data_set_id'])
            raise AirflowFailException(err_msg)
        elif ingest_status == 'CANCELLED':
            err_msg = "Ingest for data-set:{} was cancelled.".format(qs_ingest_params['data_set_id'])
            raise AirflowFailException(err_msg)
        elif ingest_status == 'COMPLETED':
            return True
        else:
            return False
