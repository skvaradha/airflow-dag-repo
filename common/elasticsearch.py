import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes.client import V1EnvVar


class ElasticsearchDAG(DAG):
    def __init__(self, name: str, description: str, params: dict, **kwargs):
        kwargs.setdefault('schedule_interval', None)
        kwargs.setdefault('start_date', datetime(2022, 4, 27))
        kwargs.setdefault('catchup', False)
        kwargs.setdefault('concurrency', 1)
        kwargs.setdefault('default_args', dict(
            namespace="airflow",
            startup_timeout_seconds=500,
            in_cluster=True,
            service_account_name="pod-user",
            owner="airflow",
            log_events_on_failure=False,
            get_logs=True,
            is_delete_operator_pod=True,
            termination_grace_period=0,
            depends_on_past=False,
            retries=0
        ))

        super(ElasticsearchDAG, self).__init__(
            dag_id=name,
            description=description,
            params=params,
            **kwargs
        )


class BaseElasticsearchOperator(KubernetesPodOperator):
    def __init__(self, dag: DAG, **kwargs):
        kwargs.setdefault('image', "733509834600.dkr.ecr.us-east-1.amazonaws.com/finops-tasks:0.2.49")
        kwargs.setdefault('do_xcom_push', True)
        kwargs.setdefault('env_vars', [
           V1EnvVar(name="XCOM_PUSH_ENABLED", value="True"),
           V1EnvVar(name="AWS_DEFAULT_REGION", value="us-east-1"),
           V1EnvVar(name="FINOPS_ES_URL", value=os.getenv("ES_FACT_DATA2")),
           V1EnvVar(name="CUSTOMER_SERVICE_URL", value=os.getenv("CUSTOMER_SERVICE_URL"))
        ])

        super(BaseElasticsearchOperator, self).__init__(dag=dag, **kwargs)


class ReindexOperator(BaseElasticsearchOperator):
    def __init__(self, dag: DAG, **kwargs):
        super(ReindexOperator, self).__init__(
            dag=dag,
            name='elasticsearch-reindex',
            cmds=[
                "finops", "elasticsearch", "reindex",
                "--organization", "{{ dag_run.conf['organization'] }}",
                "--source-alias", "{{ dag_run.conf['source_alias'] }}",
                "{{ '--source-is-shared' if dag_run.conf['source_is_shared'] else '--source-is-not-shared' }}",
                "--destination-index", "{{ dag_run.conf['destination_index'] }}",
                "{{ '--destination-is-shared' if dag_run.conf['destination_is_shared'] else '--destination-is-not-shared' }}",
                "{{ '--allow-pre-existing-destination' if dag_run.conf['allow_pre_existing_destination'] else '--no-allow-pre-existing-destination' }}",
                "--shards", "{{ dag_run.conf['shards'] }}",
                "--replicas", "{{ dag_run.conf['replicas'] }}",
                "--quiet"
            ],
            **kwargs
        )


class ReindexCleanupOperator(BaseElasticsearchOperator):
    def __init__(self, dag: DAG, **kwargs):
        super(ReindexCleanupOperator, self).__init__(
            dag=dag,
            name='elasticsearch-reindex-cleanup',
            cmds=[
                "finops", "elasticsearch", "post-reindex-cleanup",
                "--organization", "{{ dag_run.conf['organization'] }}",
                "--source-alias", "{{ dag_run.conf['source_alias'] }}",
                "{{ '--source-is-shared' if dag_run.conf['source_is_shared'] else '--source-is-not-shared' }}",
                "--destination-index", "{{ dag_run.conf['destination_index'] }}",
                "--quiet"
            ],
            **kwargs
        )


class DeleteOperator(BaseElasticsearchOperator):
    def __init__(self, dag: DAG, **kwargs):
        super(DeleteOperator, self).__init__(
            dag=dag,
            name='elasticsearch-delete-org',
            cmds=[
                "finops", "elasticsearch", "delete-org",
                "--organization", "{{ dag_run.conf['organization'] }}",
                "--alias", "{{ dag_run.conf['alias'] }}",
                "{{ '--in-shared-index' if dag_run.conf['in_shared_index'] else '--not-in-shared-index' }}",
                "--quiet"
            ],
            **kwargs
        )


class ElasticsearchTaskSensor(BaseSensorOperator, BaseElasticsearchOperator):
    TASK_TYPES = ['reindex', 'delete']

    def __init__(self, dag: DAG, elasticsearch_task_id: str, elasticsearch_task_type: str, **kwargs):
        if elasticsearch_task_type not in self.TASK_TYPES:
            raise ValueError(f'{elasticsearch_task_type} task type not in supported task types ({self.TASK_TYPES})')

        self.elasticsearch_task_type = elasticsearch_task_type
        self.elasticsearch_task_id = elasticsearch_task_id

        super(ElasticsearchTaskSensor, self).__init__(
            dag=dag,
            name=f"elasticsearch-{self.elasticsearch_task_type}-status",
            cmds=[
                "finops", "elasticsearch", f"{self.elasticsearch_task_type}-status",
                "--task-id", elasticsearch_task_id
            ],
            **kwargs
        )

    def poke(self, context):
        if self.elasticsearch_task_id is None:
            # handle case where previous elasticsearch operator did not initiate an async task
            return True

        result = BaseElasticsearchOperator.execute(self, context)

        if result['error']:
            err_msg = f"{self.elasticsearch_task_type.capitalize()} task failed: {result['error']}"
            raise AirflowFailException(err_msg)
        else:
            return result['completed']
