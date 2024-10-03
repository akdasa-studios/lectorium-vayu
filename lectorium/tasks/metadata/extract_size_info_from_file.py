from os.path import getsize

from airflow.decorators import task


@task(task_display_name="Get File Size")
def extract_size_info_from_file(path: str) -> int:
    return getsize(path)
