from airflow.exceptions import AirflowSkipException
from airflow.decorators import task


@task(
    task_display_name="Generate Title")
def generate_title(
    transcript: dict,
) -> str:
    "generated title"
