from urllib.parse import urlparse

from airflow.decorators import task
from airflow.models import Variable
from vastai import VastAI

from lectorium.ssh import SshConnection


@task(task_display_name="VastAI: Get SSH Connection")
def vastai_get_ssh_connection_to_instance(instance_id: int) -> SshConnection:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_private_ssh_key = Variable.get("vastai_private_ssh_key")
    vast_sdk = VastAI(api_key=vast_api_key)
    ssh_url = vast_sdk.ssh_url(id=instance_id)
    print(f"ssh_url: {ssh_url}")

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    parsed_url = urlparse(ssh_url)
    return SshConnection(
        host=parsed_url.hostname,
        port=parsed_url.port,
        username=parsed_url.username,
        private_key=vast_private_ssh_key,
    )
