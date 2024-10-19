from airflow.decorators import task

import lectorium.vastai as vastai


@task.branch(task_display_name="VastAI: Has Active Instances")
def has_active_instances(
    instances: list[vastai.Instance],
    success_branch_tasks: list[str],
    fail_branch_tasks: list[str],
) -> list[vastai.Instance]:
    active_instances = [
        instance for instance in instances if instance["status"] == "running"
    ]
    if len(active_instances) > 0:
        return success_branch_tasks
    else:
        return fail_branch_tasks
