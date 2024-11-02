from random import choice

from airflow.decorators import task

from services.vastai.models.instance import Instance


@task(
    task_display_name="🚀 VastAI: Get Active Instances",
    multiple_outputs=False)
def get_active_instances(
    instances: list[Instance],
    label: str
) -> Instance:
    return [
        instance
        for instance in instances
        if instance["status"] == "running" and instance["label"] == label
    ]
