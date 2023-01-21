from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from airflow.hooks.base import BaseHook

SLACK_CONN_ID = 'slack'


def task_fail_slack_alert(message: str, context):
    log_url: str = context.get('task_instance').log_url
    log_url = log_url.replace("http://localhost:8080", "https://airflow.eco.internal.spotinst.io")
    log_url = "<{log_url}|logs>".format(log_url=log_url)

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: *{message}* 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        message=message,
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow'
    )
    return failed_alert.execute(context=context)