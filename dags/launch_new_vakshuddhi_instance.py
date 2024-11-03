from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from pendulum import duration

import services as services
import lectorium as lectorium
import services.vastai as vastai
import services.ssh as ssh


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🔉 Vakshuddhi: Launch New Instance",
    description="Launches new instance on vast.ai",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lectorium", "audio"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 0,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=5),
        "max_retry_delay": duration(hours=2),
    },
    render_template_as_native_obj=True,

)
def launch_new_vakshuddhi_instance():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    vastai_api_key = (
        Variable.get(lectorium.config.VASTAI_ACCESS_KEY)
    )

    vastai_private_ssh_key = (
        Variable.get(lectorium.config.VASTAI_PRIVATE_SSH_KEY)
    )

    commands_to_configure_instance = [
        "DEBIAN_FRONTEND=noninteractive apt-get -y -qq update",
        "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install apt-utils > /dev/null 2>&1",
        "curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash",
        "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install -y ffmpeg git-lfs curl python3-pip python3-venv > /dev/null 2>&1",
        "pip3 install resemble-enhance --upgrade 2>&1",
    ]


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # ---------------------------- Launch New Instance --------------------------- #

    instance_id = (
        vastai.launch_new_instance(
            vast_api_key=vastai_api_key,
            query="cuda_vers=12.4 num_gpus=1 gpu_name=RTX_4090 inet_down>=100 rentable=true geolocation=EU",
            image="pytorch/pytorch:2.4.0-cuda12.4-cudnn9-devel",
            label="vakshuddhi",
            disk=32,
        )
    )

    waited_instance_id = (
        vastai.wait_instance_to_start(
            vast_api_key=vastai_api_key,
            instance_id=instance_id,
        )
    )

    # --------------------------- Configure Instance ----------------------------- #

    connection = (
        vastai.get_ssh_connection_to_instance(
            vastai_api_key, instance_id=instance_id
        )
    )

    executed_commands = (
        ssh.run_commands(
            url=connection,
            private_key=vastai_private_ssh_key,
            commands=commands_to_configure_instance,
            fail_on_stderr=True,
            timeout=60 * 25,
        )
    )


    instance_id >> waited_instance_id >> connection >> executed_commands

launch_new_vakshuddhi_instance()
