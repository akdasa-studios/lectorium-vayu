from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from pendulum import duration

import services.aws as aws
import services.vastai as vastai
import services.ssh as ssh
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🔈 Audio: Normalize",
    description="Process audio file",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "audio"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 1,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=2),
        "max_retry_delay": duration(hours=2),
    },
    render_template_as_native_obj=True,
    params={
        "track_id": Param(
            description="Track ID to process",
            type="string",
            title="Track ID",
        ),
        "path_source": Param(
            description="Path to file to process",
            type="string",
            title="Source Path",
        ),
        "path_original_dest": Param(
            description="Upload original file to this path",
            type="string",
            title="Upload Original Path",
        ),
        "path_processed_dest": Param(
            description="Upload processed file to this path",
            type="string",
            title="Upload Processed Path",
        ),
    },
)
def process_audio():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id = "{{ dag_run.conf['track_id'] }}"
    path_source = "{{ dag_run.conf['path_source'] }}"
    path_original_dest = "{{ dag_run.conf['path_original_dest'] }}"
    path_processed_dest = "{{ dag_run.conf['path_processed_dest'] }}"

    vastai_access_key      = Variable.get(lectorium.config.VASTAI_ACCESS_KEY)
    vastai_private_ssh_key = Variable.get(lectorium.config.VASTAI_PRIVATE_SSH_KEY)


    # ---------------------------------------------------------------------------- #
    #                                     Tasks                                    #
    # ---------------------------------------------------------------------------- #

    vastai_instances = vastai.get_instances(vastai_access_key)
    vastai_active_instance = vastai.get_active_instance(vastai_instances, "audio")
    vastai_ssh_connection = (
        vastai.get_ssh_connection_to_instance(
            api_key=vastai_access_key,
            instance_id=vastai_active_instance["id"])
    )

    # ---------------------------------------------------------------------------- #
    #                           Download and Upload Files                          #
    # ---------------------------------------------------------------------------- #

    download_original_file = (
        task(
            task_display_name="⬇️ Download File To Process"
        )(
            ssh.actions.run_commands
        )(
            fail_on_stderr=True,
            url=vastai_ssh_connection,
            private_key=vastai_private_ssh_key,
            commands=[
                 f"curl -sS --create-dirs -X GET '{path_source}' -o {track_id}/in/{track_id}.mp3",
            ]
        )
    )

    upload_original_file = (
        task(
            task_display_name="⬆️ Upload Original File"
        )(
            ssh.actions.run_commands
        )(
            fail_on_stderr=True,
            url=vastai_ssh_connection,
            private_key=vastai_private_ssh_key,
            commands=[
                f"curl -sS -X PUT --upload-file '{track_id}/in/{track_id}.mp3' '{path_original_dest}'",
            ]
        )
    )

    upload_processed_file = (
        task(
            task_display_name="⬆️ Upload Processed File"
        )(
            ssh.actions.run_commands
        )(
            fail_on_stderr=True,
            url=vastai_ssh_connection,
            private_key=vastai_private_ssh_key,
            commands=[
                f"curl -sS -X PUT --upload-file '{track_id}/in/{track_id}.mp3' '{path_processed_dest}'",
            ]
        )
    )


    # ---------------------------------------------------------------------------- #
    #                                 Process File                                 #
    # ---------------------------------------------------------------------------- #

    process_file = (
       task(
            task_display_name="🔊 Process Audio File"
        )(
            ssh.actions.run_commands
        )(
            url=vastai_ssh_connection,
            private_key=vastai_private_ssh_key,
            commands=[
                f"cd {track_id}/in;"
                f"ffmpeg -v quiet -stats -n -i {track_id}.mp3 {track_id}.wav",
                f"cd {track_id};" f"resemble-enhance ./in ./out --denoise_only",
                f"cd {track_id}/out; ffmpeg -v quiet -stats -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3",
            ]
        )
    )

    download_original_file >> process_file >> upload_processed_file
    download_original_file >> upload_original_file

process_audio()
