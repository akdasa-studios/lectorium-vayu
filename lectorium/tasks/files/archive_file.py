from airflow.decorators import task


@task(task_display_name="Archive File")
def archive_file(
    file_path: str,
) -> str:
    pass
