from typing import Callable
from cuid2 import cuid_wrapper
from airflow.decorators import task

CUID_GENERATOR: Callable[[], str] = cuid_wrapper()


@task(
    task_display_name="Generate ID")
def generate_track_id():
    return CUID_GENERATOR()
