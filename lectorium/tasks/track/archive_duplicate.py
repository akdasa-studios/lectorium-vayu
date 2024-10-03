from airflow.decorators import task

@task(
    task_display_name="Archive Duplicate")
def archive_duplicate(file_path: str):
    print(f"Archiving duplicate: {file_path}")
