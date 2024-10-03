from airflow.decorators import task


@task(
    task_display_name="Complete")
def process_complete(
) -> str:
    pass
