from random import choice

from airflow.decorators import task

from services.vastai.models.instance import Instance


@task(
    task_display_name="🚀 VastAI: Get Active Instances",
    multiple_outputs=True,
)
def get_active_instance(
    instances: list[Instance],
    label: str
) -> Instance:
    return choice([
        instance
        for instance in instances
        if instance["status"] == "running" and instance["label"] == label
    ])
