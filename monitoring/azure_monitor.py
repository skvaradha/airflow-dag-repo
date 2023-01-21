"""
Any changes here should also be made in finops-mwaa repo.
"""

import os

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook

from monitoring.finops_monitor import finops_monitor

AZURE_SLACK_CONN_ID = "azure_errors_slack_channel"


def azure_failure_monitor_alert(context):
    """
    call finops monitor
    write alert to slack channel
    """
    send_slack_alert(context=context)
    finops_monitor(context=context)


def send_slack_alert(context):
    log_url: str = context.get("task_instance").log_url
    log_url = log_url.replace("http://localhost:8080", "https://airflow.eco.internal.spotinst.io")
    slack_webhook_token = BaseHook.get_connection(AZURE_SLACK_CONN_ID).password
    environment = os.getenv("ENVIRONMENT", "")
    attachments = [
        {
            "mrkdwn_in": ["text"],
            "color": "#87ceff",
            "pretext": f"*Airflow DAG Failed - {environment.upper()}*",
            "fields": [
                {"value": f"*DAG*: {context.get('task_instance').dag_id}", "short": False},
                {"value": f"*Task*: {context.get('task_instance').task_id}", "short": False},
                {"value": f"*Execution Time*: {context.get('execution_date')}", "short": False},
                {"value": f"*Log*: <{log_url}|logs>", "short": False},
            ],
        }
    ]

    failed_alert = SlackWebhookOperator(
        task_id="azure_task_alert",
        http_conn_id=AZURE_SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        attachments=attachments,
        username="airflow",
    )
    return failed_alert.execute(context=context)
