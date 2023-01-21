from airflow.providers.amazon.aws.operators.step_function_start_execution import StepFunctionStartExecutionOperator
from airflow.utils.task_group import TaskGroup

from common.step_function_sensor import FinopsStepFunctionExecutionSensor

'''Removing xcom push to function better as a smart sensor'''


class StepFunctionRunner(TaskGroup):

    def __init__(self, state_machine_input_xcom: str, state_machine_arn: str, pool: str = "default_pool", **kwargs):
        super().__init__(**kwargs)
        self.state_machine_input_xcom = state_machine_input_xcom
        self.state_machine_arn = state_machine_arn
        self.pool = pool

    def trigger_step_function(self):
        return StepFunctionStartExecutionOperator(
            pool=self.pool,
            task_id='start_step_function',
            state_machine_arn=self.state_machine_arn,
            state_machine_input="{{{{ task_instance.xcom_pull(task_ids='{}', key='return_value') }}}}".format(
                self.state_machine_input_xcom),
            do_xcom_push=True
        )

    def watch_step_function(self):
        trigger_step_function_task_id = f"{self.group_id}.start_step_function"
        return FinopsStepFunctionExecutionSensor(
            pool=self.pool,
            task_id='watch_step_function',
            execution_arn="{{{{ task_instance.xcom_pull(task_ids='{}', key='return_value') }}}}".format(
                trigger_step_function_task_id)
        )

    def build_default_workflow(self):
        return self.trigger_step_function() >> self.watch_step_function()
