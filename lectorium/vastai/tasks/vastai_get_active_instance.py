from random import choice

from airflow.decorators import task

from lectorium.vastai.actions import get_instances
from lectorium.vastai.models.instance import VastAiInstance


@task(task_display_name="VastAI: Get Active Instances")
def vastai_get_active_instance(label: str) -> VastAiInstance:
    instances = get_instances()
    return choice([
        instance
        for instance in instances
        if instance["status"] == "running" and instance["label"] == label
    ])
