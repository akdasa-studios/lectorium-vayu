from airflow.decorators import task
from airflow.models import Variable
from vastai import VastAI


@task(task_display_name="Instance Status")
def vastai_instance_status(instance_id: int):

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    response = vast_sdk.show_instance(id=instance_id)
    print(response)
