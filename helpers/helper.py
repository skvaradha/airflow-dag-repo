from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.utils.state import State
from monitoring.sns_publish import sns_msg


def get_last_months(months):
    start_date = datetime.now()
    for i in range(months):
        yield start_date.year, start_date.month
        start_date += relativedelta(months=-1)


def final_status(**kwargs) -> None:
    """this is a function to prep a final task check for a dag
    it will determine if any of the tasks have a status other than
    success or skipped, and if so, it will send a msg to sns
    (by default this sns will go to a slack channel for MSP (and only msp),
    but in the dag you can add a different arn to kwargs.
    The function will then raise an error to fail itself.

    This is to ensure the dag itself fails, in the event of
    a situation where the end of the dag is something like
    the EMR killer task"""

    dagid = kwargs['dag'].dag_id

    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() not in [State.SUCCESS, State.SKIPPED] and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            taskid = task_instance.task_id

            # default sns variables
            if not kwargs.get("arn", False):
                kwargs["arn"] = "arn:aws:sns:us-east-1:884866656237:MSP_errors"

            if not kwargs.get("msg", False):
                kwargs["msg"] = f"Task {taskid} failed within MSP {dagid}. Failing this DAG run"

            if not kwargs.get("subject", False):
                kwargs["subject"] = f"Task {taskid} failed. Failing this DAG run"

            if not kwargs.get("sns_off", False):
                sns_msg(kwargs["arn"],
                        kwargs["msg"],
                        kwargs["subject"])
            raise Exception(f"Task {taskid} failed. Failing this DAG run")
