import re

from airflow.decorators import task
from airflow.models import Variable
from vastai import VastAI

from lectorium.vastai import InstanceParams


@task(task_display_name="VastAI: Launch New Instance")
def launch_new_instance(
    instance_params: InstanceParams,
) -> int:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    new_instance_id = None

    response = vast_sdk.launch_instance(
        num_gpus=str(instance_params["num_gpus"]),
        gpu_name=instance_params["gpu_name"],
        image=instance_params["image"],
        extra=instance_params["extra"],
        disk=instance_params["disk"],
    )

    # response example:
    # Started. {'success': True, 'new_contract': 13096176}
    # Instance launched successfully: 13096176
    match = re.search(r"Instance launched successfully: (\d+)", response)
    if match:
        new_instance_id = int(match.group(1))
        print(f"Instance launched successfully: {new_instance_id}")
    else:
        print("Failed to parse the response.")

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    if not new_instance_id:
        raise ValueError("Failed to launch the instance.")

    return new_instance_id
