from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from pendulum import duration

import services.aws as aws
import lectorium as lectorium

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="Process Audio",
    description="Process audio file",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "audio"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
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

    generate_temporary_access_key = Variable.get(
        lectorium.config.VAR_APP_BUCKET_GENERATE_TEMPORARY_ACCESS_KEY,
        deserialize_json=True)

    # ---------------------------------------------------------------------------- #
    #                                     Tasks                                    #
    # ---------------------------------------------------------------------------- #

    session_token = (
        aws.get_session_token(
            credentials=generate_temporary_access_key)
    )


#     @task(task_display_name="Configure S3 Connection")
#     def configure_s3_connection(
#         ssh_connection: SshConnection,
#         sts_session_token: StsSessionToken,
#     ):
#         return ssh_run_commands(
#             ssh_connection,
#             [
#                 f"aws configure set aws_access_key_id {sts_session_token.access_key_id}",
#                 f"aws configure set aws_secret_access_key {sts_session_token.secret_access_key}",
#                 f"aws configure set aws_session_token {sts_session_token.token}",
#             ],
#         )



#     @task(task_display_name="⬇️ Download File To Process")
#     def download_file_to_process(
#         track_id: str,
#         path: str,
#         ssh_connection: SshConnection,
#     ):
#         cred: AppBucketAccessKey = Variable.get(
#             "app-bucket::access-key", deserialize_json=True)
#         return ssh_run_commands(
#             ssh_connection,
#             [   # TODO: .mp3 extension is hardcoded
#                 # TODO: quote path in case of spaces
#                 f"aws s3 cp '{path}' ./{track_id}/in/{track_id}.mp3 --endpoint-url={cred['s3_endpoint_url']}",
#             ],
#             fail_on_stderr=True,
#         )


#     @task(task_display_name="⚙️ Process File")
#     def process_file(
#         track_id: str,
#         ssh_connection: SshConnection
#     ):
#         return ssh_run_commands(
#             ssh_connection,
#             [
#                 f"cd {track_id}/in;"
#                 f"ffmpeg -v quiet -stats -n -i {track_id}.mp3 {track_id}.wav",
#                 f"cd {track_id};" f"resemble-enhance ./in ./out --denoise_only",
#                 f"cd {track_id}/out; ffmpeg -v quiet -stats -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3",
#             ],
#         )

#     @task(task_display_name="⬆️ Upload Original File")
#     def upload_original_file(
#         track_id: str,
#         path: str,
#         ssh_connection: SshConnection
#     ):
#         cred: AppBucketAccessKey = Variable.get(
#             "app-bucket::access-key", deserialize_json=True)
#         return ssh_run_commands(
#             ssh_connection,
#             [   # TODO: .mp3 extension is hardcoded
#                 # TODO: quote path in case of spaces
#                 f"aws s3 cp ./{track_id}/in/{track_id}.mp3 '{path}' --endpoint-url={cred['s3_endpoint_url']}",
#             ],
#             fail_on_stderr=True,
#         )

#     @task(task_display_name="⬆️ Upload Processed File")
#     def upload_processed_file(
#         track_id: str,
#         path: str,
#         ssh_connection: SshConnection
#     ):
#         cred: AppBucketAccessKey = Variable.get(
#             "app-bucket::access-key", deserialize_json=True)
#         return ssh_run_commands(
#             ssh_connection,
#             [   # TODO: .mp3 extension is hardcoded
#                 # TODO: quote path in case of spaces
#                 f"aws s3 cp ./{track_id}/out/{track_id}.mp3 '{path}' --endpoint-url={cred['s3_endpoint_url']}",
#             ],
#             fail_on_stderr=True,
#         )

#     # ---------------------------------------------------------------------------- #
#     #                                     Steps                                    #
#     # ---------------------------------------------------------------------------- #


#     session_token = sts_get_session_token(duration_seconds=20*60)
#     vastai_active_instance = vastai_get_active_instance("audio")
#     vastai_ssh_connection = vastai_get_ssh_connection_to_instance(vastai_active_instance["id"])

#     s1 = download_file_to_process(track_id, path_source, vastai_ssh_connection)
#     s2 = process_file(track_id, vastai_ssh_connection)
#     s3 = upload_original_file(track_id, path_original_dest, vastai_ssh_connection)
#     s4 = upload_processed_file(track_id, path_processed_dest, vastai_ssh_connection)

#     (
#         vastai_active_instance
#         >> vastai_ssh_connection
#         >> configure_s3_connection(vastai_ssh_connection, session_token)
#         >> s1 >> s2 >> [s3, s4]
#     )

process_audio()
