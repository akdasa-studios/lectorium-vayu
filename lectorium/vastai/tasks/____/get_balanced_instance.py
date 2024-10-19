from random import choice

from airflow.decorators import task

import lectorium.vastai as vastai


@task(task_display_name="VastAI: Get Balanced Instance")
def get_balanced_instance(
    instances: list[vastai.Instance], label: str
) -> vastai.Instance:
    return choice(list(filter(lambda instance: instance["label"] == label, instances)))
