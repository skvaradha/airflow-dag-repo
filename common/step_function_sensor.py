import json
from typing import Optional

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.step_function_execution import StepFunctionExecutionSensor
from airflow.utils.decorators import apply_defaults


class FinopsStepFunctionExecutionSensor(StepFunctionExecutionSensor):

    poke_context_fields = ('execution_arn', 'aws_conn_id', 'region_name')

    @apply_defaults
    def __init__(self, *, execution_arn: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None,
                 **kwargs):
        super().__init__(execution_arn=execution_arn, aws_conn_id=aws_conn_id, region_name=region_name, **kwargs)

    def poke(self, context):
        execution_status = self.get_hook().describe_execution(self.execution_arn)
        state = execution_status['status']
        output = json.loads(execution_status['output']) if 'output' in execution_status else None
        self.log.info(output)

        if state in self.FAILURE_STATES:
            raise AirflowException(f'Step Function sensor failed. State Machine Output: {output}')

        if state in self.INTERMEDIATE_STATES:
            return False

        return True
