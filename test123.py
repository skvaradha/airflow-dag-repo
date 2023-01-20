import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule


from finops.customer_service.models.customer import Customer
from finops.customer_service.client import CustomerServiceClient

from monitoring.finops_monitor import finops_heartbeat_monitor
from monitoring.azure_monitor import azure_failure_monitor_alert


DAG_NAME = "azure-cost-management-price-sheets-etl"
DAG_CONCURRENCY = 8

PRICE_SHEET_GENERATE_CMD = "cost-management-price-sheets-generate"
PRICE_SHEET_DOWNLOAD_CMD = "cost-management-price-sheets-download"

DOWNLOAD_URL_POLLING_TASK_NAME = "price-sheet-download-url-polling"

PAYG_PRICE_SHEET_TASK_NAME = 'payg-price-sheet'
PAYG_PRICE_TYPES = ['Reservation', 'DevTestConsumption', 'Consumption']


def get_price_sheet_task(
    cmd: str,
    customer: Customer,
    xcom_push=False
) -> KubernetesPodOperator:

    return KubernetesPodOperator(
        name=f"{customer.display_name}-{cmd}",
        image="733509834600.dkr.ecr.us-east-1.amazonaws.com/finops-tasks:latest",
        env_vars={
            "AWS_DEFAULT_REGION": "us-east-1",
            "CUSTOMER_SERVICE_URL": os.getenv("CUSTOMER_SERVICE_URL"),
            "SC_AZURE_DATA_BUCKET": os.getenv("SC_AZURE_DATA_BUCKET"),
            "AZURE_ETL_TABLE_NAME": os.getenv("AZURE_ETL_TABLE_NAME"),
        },
        cmds=[
            "finops",
            "azure",
            cmd,
            "--customer",
            customer.display_name,
            "--date",
            f"{{{{ dag_run.start_date.strftime('%Y-%m-%d') }}}}",
        ],
        task_id=f"{customer.display_name}-{cmd}",
        on_failure_callback=azure_failure_monitor_alert,
        on_success_callback=finops_heartbeat_monitor,
        do_xcom_push=xcom_push,
    )


def get_payg_price_sheet_task(price_type: str, dag) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        dag=dag,
        name=f"{PAYG_PRICE_SHEET_TASK_NAME}-{price_type}",
        image="733509834600.dkr.ecr.us-east-1.amazonaws.com/finops-tasks:latest",
        env_vars={
            "AWS_DEFAULT_REGION": "us-east-1",
            "SC_AZURE_DATA_BUCKET": os.getenv("SC_AZURE_DATA_BUCKET"),
        },
        cmds=[
            "finops",
            "azure",
            PAYG_PRICE_SHEET_TASK_NAME,
            "--price_type",
            price_type,
            "--date",
            f"{{{{ dag_run.start_date.strftime('%Y-%m-%d') }}}}",
        ],
        task_id=f"{PAYG_PRICE_SHEET_TASK_NAME}-{price_type}",
        on_failure_callback=azure_failure_monitor_alert,
        on_success_callback=finops_heartbeat_monitor,
        do_xcom_push=False,
    )


def add_athena_partition_task(customer: Customer):
    cmd = "add-mca-table-partition"
    database_name = os.getenv("AZURE_ATHENA_DATABASE")
    table = "cost_management_price_sheets"

    return KubernetesPodOperator(
        dag=dag,
        name=cmd,
        image="733509834600.dkr.ecr.us-east-1.amazonaws.com/finops-tasks:latest",
        env_vars={
            "AWS_DEFAULT_REGION": "us-east-1",
            "CUSTOMER_SERVICE_URL": os.getenv("CUSTOMER_SERVICE_URL"),
            "AZURE_ETL_TABLE_NAME": os.getenv("AZURE_ETL_TABLE_NAME"),
        },
        cmds=[
            "finops",
            "azure",
            cmd,
            "--customer",
            customer.display_name,
            "--date",
            "{{ dag_run.start_date.strftime('%Y-%m-%d') }}",
            "--database",
            database_name,
            "--table",
            table,
        ],
        task_id=f"{customer.display_name}-{cmd}-{table}",
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
        on_failure_callback=azure_failure_monitor_alert,
        on_success_callback=finops_heartbeat_monitor,
    )


def run_glue_crawler(cmd: str) -> AwsGlueCrawlerOperator:
    return AwsGlueCrawlerOperator(config=dict(Name=cmd), task_id=f"{cmd}_glue_crawler")


def response_check(response, task_instance):
    print(response)

    if response.status_code == 200:
        return True
    elif response.status_code == 202:
        return False
    else:
        raise Exception


def get_download_url_polling_sensor(customer):
    previous_task_id = f"{customer.display_name}-{PRICE_SHEET_GENERATE_CMD}"
    download_url_var_name = "download_location_url"
    bearer_token_var_name = "bearer_token"

    return HttpSensor(
        task_id=f"{customer.display_name}-{DOWNLOAD_URL_POLLING_TASK_NAME}",
        http_conn_id='',
        method="GET",
        endpoint=f"{{{{ti.xcom_pull(task_ids='{previous_task_id}')['{download_url_var_name}']}}}}",
        headers=dict(Authorization=f"{{{{ti.xcom_pull(task_ids='{previous_task_id}')['{bearer_token_var_name}']}}}}"),
        response_check=response_check,
    )


# Retrieve MCA customers
customer_client = CustomerServiceClient(os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:5000"))
azure_customers = customer_client.get().all(is_azure_customer=True)
mca_customers = [customer for customer in azure_customers
                 if customer.azure_metadata.get("microsoft_agreement_type", "") == "Microsoft Customer Agreement"
                 ]

with DAG(
    DAG_NAME,
    catchup=False,
    concurrency=DAG_CONCURRENCY,
    default_args=dict(
        namespace="airflow",
        service_account_name="pod-user",
        startup_timeout_seconds=500,
        log_events_on_failure=False,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        termination_grace_period=0,
        priority_weight=2,
        owner="airflow",
        depends_on_past=False,
        retries=2,
        retry_delay=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    ),
    description=f"Retrieve Azure Customer price sheet data via Cost Management API",
    schedule_interval='0 6 1,14 * *',
    start_date=datetime(2022, 9, 1),
) as dag:

    for cur_customer in mca_customers:
        generate_price_sheet = get_price_sheet_task(PRICE_SHEET_GENERATE_CMD, cur_customer, xcom_push=True)
        sensor = get_download_url_polling_sensor(cur_customer)
        download_price_sheet = get_price_sheet_task(PRICE_SHEET_DOWNLOAD_CMD, cur_customer)
        add_athena_partition = add_athena_partition_task(cur_customer)

        generate_price_sheet >> sensor >> download_price_sheet >> add_athena_partition

    payg_tasks = [get_payg_price_sheet_task(item, dag) for item in PAYG_PRICE_TYPES]
    payg_tasks.append(run_glue_crawler("azure_payg_crawlers"))
    chain(*payg_tasks)
