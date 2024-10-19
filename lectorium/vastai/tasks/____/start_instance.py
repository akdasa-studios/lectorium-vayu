from airflow.decorators import task
from airflow.models import Variable
from vastai import VastAI


@task(task_display_name="VastAI: Start Instance")
def start_instance(
    instance_id: int,
):

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # response = vast_sdk.start_instance(ID=instance_id)
    # print(response)

    return 13100788
