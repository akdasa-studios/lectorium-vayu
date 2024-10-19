from time import sleep

from airflow.decorators import task
from airflow.models import Variable
from vastai import VastAI


@task.branch(
    task_display_name="VastAI: Wait Instances To Start", trigger_rule="one_success"
)
def wait_instances_to_start(
    instance_ids: list[int],
    instance_started_tasks: list[str],
    instance_not_started_tasks: list[str],
    minutes=5,
):

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    waiting_instances = [i for i in instance_ids if i]

    for _ in range(minutes):
        instances_started_count = 0

        for instance_id in waiting_instances:
            response = vast_sdk.show_instance(id=instance_id)
            if "running" in response:
                instances_started_count += 1
            else:
                print(f"Instance {instance_id} is not running yet.")
            sleep(60)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    if waiting_instances == len(waiting_instances):
        return instance_started_tasks
    else:
        return instance_not_started_tasks
