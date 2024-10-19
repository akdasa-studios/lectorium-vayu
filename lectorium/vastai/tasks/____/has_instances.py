from airflow.decorators import task

import lectorium.vastai as vastai


@task.branch(task_display_name="VastAI: Has Instances")
def has_instances(
    instances: list[vastai.Instance],
    success_branch_tasks: list[str],
    fail_branch_tasks: list[str],
) -> list[vastai.Instance]:
    if len(instances) > 0:
        return success_branch_tasks
    else:
        return fail_branch_tasks
