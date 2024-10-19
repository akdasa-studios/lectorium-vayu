from airflow.decorators import task


@task(task_display_name='Flatten')
def flatten(data: list):
    r = []
    for i in data:
        r.extend(i)
    return r
