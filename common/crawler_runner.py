from airflow.utils.task_group import TaskGroup

from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import AwsGlueCrawlerSensor
from monitoring.finops_monitor import finops_heartbeat_monitor, finops_monitor


class CrawlerRunner(TaskGroup):
    def __init__(self, config: dict, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.crawler_name = config.get("Name")

    def trigger_crawler(self):
        return AwsGlueCrawlerOperator(
            task_id=f"start_crawler_{self.crawler_name}",
            config=self.config,
            on_failure_callback=finops_monitor,
            on_success_callback=finops_heartbeat_monitor,
        )

    def watch_crawler(self):
        return AwsGlueCrawlerSensor(
            task_id=f"crawler_sensor_{self.crawler_name}",
            crawler_name=self.crawler_name,
        )

    def build_default_workflow(self):
        return self.trigger_crawler() >> self.watch_crawler()
