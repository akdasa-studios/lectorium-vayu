from airflow.decorators import task
from airflow.exceptions import AirflowSkipException


@task(task_display_name="Generate Title")
def generate_title(
    transcript: dict,
) -> str:
    "generated title"
