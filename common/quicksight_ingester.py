import boto3

from uuid import uuid4

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from common.quicksight_ingest_sensor import QuickSightIngestionSensor


class QuickSightIngester(TaskGroup):
    def __init__(
        self,
        data_set_id: str,
        aws_account_id: str,
        trigger_rule: str = TriggerRule.ALL_DONE,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.data_set_id = data_set_id
        self.aws_account_id = aws_account_id
        self.boto3_qs_client = boto3.client("quicksight")
        self.trigger_rule = trigger_rule

    def trigger_ingestion(self, **context):
        qs_client = context["boto3_qs_client"]
        data_set_id = context["data_set_id"]
        aws_account_id = context["aws_account_id"]
        ingest_id = str(uuid4())

        response = qs_client.create_ingestion(
            DataSetId=data_set_id, AwsAccountId=aws_account_id, IngestionId=ingest_id
        )

        if response["Status"] != 201:
            raise AirflowFailException("Failed to trigger ingest f".format())

        return {
            "aws_account_id": aws_account_id,
            "data_set_id": data_set_id,
            "ingest_id": ingest_id,
        }

    def create_ingestion(self):
        # call boto 3 with data_set_id, account_id from the env, ingest_id generated
        # return {data_set_id, account_id, ingest_id}
        return PythonOperator(
            task_id="create_quicksight_ingestion",
            python_callable=self.trigger_ingestion,
            op_kwargs={
                "aws_account_id": self.aws_account_id,
                "data_set_id": self.data_set_id,
                "boto3_qs_client": self.boto3_qs_client,
            },
            trigger_rule=self.trigger_rule,
        )

    def watch_ingestion(self):
        return QuickSightIngestionSensor(
            create_quicksight_ingestion_task_group=self.group_id,
            task_id="watch_quicksight_ingestion",
        )

    def build_trigger_only_workflow(self):
        return self.create_ingestion()

    def build_default_workflow(self):
        return self.create_ingestion() >> self.watch_ingestion()
