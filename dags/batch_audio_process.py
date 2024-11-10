from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

import services.aws as aws
import services.vastai as vastai
import services.ssh as ssh
import services.couchdb as couchdb
import lectorium as lectorium

from lectorium.tracks_inbox import TrackInbox

from lectorium.config import (
    LECTORIUM_DATABASE_COLLECTIONS,
    LECTORIUM_DATABASE_CONNECTION_STRING,
    LECTORIUM_VAKSHUDDKI_VASTAI_QUERY,
    VASTAI_ACCESS_KEY,
    VASTAI_PRIVATE_SSH_KEY,
    VAR_APP_BUCKET_NAME,
    VAR_APP_BUCKET_ACCESS_KEY,
)


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🔈 Audio: Batch Process",
    description="Process audio files in batch",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "audio"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
    },
    render_template_as_native_obj=True,
    max_active_runs=1,
)
def batch_audio_process():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    vastai_access_key       = Variable.get(VASTAI_ACCESS_KEY)
    vastai_private_ssh_key  = Variable.get(VASTAI_PRIVATE_SSH_KEY)
    database_collections    = Variable.get(LECTORIUM_DATABASE_COLLECTIONS, deserialize_json=True)
    database_connection     = Variable.get(LECTORIUM_DATABASE_CONNECTION_STRING)
    vakshuddhi_vastai_query = Variable.get(LECTORIUM_VAKSHUDDKI_VASTAI_QUERY)
    app_bucket_name         = Variable.get(VAR_APP_BUCKET_NAME)
    app_bucket_creds        = Variable.get(VAR_APP_BUCKET_ACCESS_KEY, deserialize_json=True)


    # ---------------------------------------------------------------------------- #
    #                                    Helpers                                   #
    # ---------------------------------------------------------------------------- #

    def sign_url(method: str, url: str):
        return aws.actions.sign_url(
            credentials=app_bucket_creds,
            bucket_name=app_bucket_name,
            object_key=url,
            method=method.lower(),
            expiration=60*10)


    # ---------------------------------------------------------------------------- #
    #                             Get Files To Process                             #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📥 Get Inbox Tracks To Process")
    def get_inbox_tracks_to_process():
        # get tracks that are ready to be processed
        documents: TrackInbox = couchdb.actions.find_documents(
            connection_string=database_connection,
            collection=database_collections["tracks_inbox"],
            filter={
                "$and": [
                    { "status": "ready" },  # track is ready to be processed
                    { "tasks.process_audio": { "$exists": False } }  # audio has not been processed
                ]
            }
        )

        # skip if no documents to process
        if not documents:
            raise AirflowSkipException("No documents to process")

        # set task process_audio to processing
        for document in documents:
            if "tasks" not in document:
                document["tasks"] = {}
            document["tasks"]["process_audio"] = "processing"

        # save changes
        for document in documents:
            couchdb.actions.save_document(
                connection_string=database_connection,
                collection=database_collections["tracks_inbox"],
                document=document
            )

        # return documents to process
        return [
            {
                "_id": document["_id"],
                "source": document["source"],
            } for document in  documents
        ]

    # ---------------------------------------------------------------------------- #
    #                        Launch New Vakshuddhi Instance                        #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="🚀 Launch Vakshuddhi Instance",
        retries=3, retry_delay=timedelta(minutes=1),
        pool="vakshuddhi::process-audio")
    def launch_new_vakshuddhi_instance(
        vastai_access_key: str,
        vastai_private_ssh_key: str,
        vakshuddhi_vastai_query: str,
        commands_to_configure_instance: list[str]
    ):
        instance_id = vastai.launch_new_instance(
            vast_api_key=vastai_access_key,
            query=vakshuddhi_vastai_query,
            image="pytorch/pytorch:2.4.0-cuda12.4-cudnn9-devel",
            label="vakshuddhi",
            disk=32,
        )

        vastai.wait_instance_to_start(
            vast_api_key=vastai_access_key,
            instance_id=instance_id,
        )

        connection = vastai.get_ssh_connection_to_instance(
            vastai_access_key,
            instance_id=instance_id
        )

        ssh.run_commands(
            url=connection,
            private_key=vastai_private_ssh_key,
            commands=commands_to_configure_instance,
            fail_on_stderr=True,
            timeout=60 * 25,
        )

        return instance_id

    # ---------------------------------------------------------------------------- #
    #                                 Process File                                 #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="🔊 Process Audio File",
        retries=3,
        retry_delay=timedelta(minutes=1))
    def process_audio_file(
        track: dict,
        ssh_connection: str,
        ssh_private_key: str,
    ):
        track_id     = track["_id"]
        track_source = track["source"]

        # sign urls for download and upload from/to S3 bucket
        uri_download         = sign_url("GET", track_source)
        uri_upload_original  = sign_url("PUT", f"library/audio/original/{track_id}.mp3")
        uri_upload_processed = sign_url("PUT", f"library/audio/normalized/{track_id}.mp3")

        # execute commands on vakshuddhi instance to process audio file
        ssh.actions.run_commands(
            fail_on_stderr=True,
            url=ssh_connection,
            private_key=ssh_private_key,
            timeout=60 * 15,
            commands=[
                # download file to process
                f"curl --create-dirs -X GET '{uri_download}' -o {track_id}/in/{track_id}.mp3 --progress-bar 2>&1",

                # process file
                f"cd {track_id}/in;"
                f"ffmpeg -v quiet -stats -n -i {track_id}.mp3 {track_id}.wav",
                f"cd {track_id};" f"resemble-enhance ./in ./out --denoise_only",
                f"cd {track_id}/out; ffmpeg -v quiet -stats -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3",

                # upload original and processed files
                f"curl -X PUT --upload-file '{track_id}/in/{track_id}.mp3' '{uri_upload_original}' --progress-bar 2>&1",
                f"curl -X PUT --upload-file '{track_id}/out/{track_id}.mp3' '{uri_upload_processed}' --progress-bar 2>&1",
            ]
        )

        # update track status to processed
        document: TrackInbox = couchdb.actions.get_document(
            connection_string=database_connection,
            collection=database_collections["tracks_inbox"],
            document_id=track_id,
        )

        document["tasks"]["process_audio"] = "done"

        couchdb.actions.save_document(
            connection_string=database_connection,
            collection=database_collections["tracks_inbox"],
            document=document
        )

    # ---------------------------------------------------------------------------- #
    #                           Stop Vakshuddhi Instance                           #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="🛑 Shutdown Vakshuddhi Instance")
    def shutdown_vakshuddhi_instance(
        vastai_access_key: str,
        instance_id: int,
    ):
        vastai.shutdown_instance(
            vastai_access_key,
            instance_id
        )

    # ---------------------------------------------------------------------------- #
    #                                     Flow                                     #
    # ---------------------------------------------------------------------------- #

    (
        (
            tracks_to_process := get_inbox_tracks_to_process().as_setup()
        ) >> (
            vakkshuddhi_instance_id := launch_new_vakshuddhi_instance(
                vastai_access_key,
                vastai_private_ssh_key,
                vakshuddhi_vastai_query,
                commands_to_configure_instance = [
                    "DEBIAN_FRONTEND=noninteractive apt-get -y -qq update",
                    "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install apt-utils > /dev/null 2>&1",
                    "curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash",
                    "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install -y ffmpeg git-lfs curl python3-pip python3-venv > /dev/null 2>&1",
                    "pip3 install resemble-enhance --upgrade 2>&1",
                ]
            )
        ) >> (
            process_audio_file
                .partial(
                    ssh_connection=vakkshuddhi_instance_id,
                    ssh_private_key=vastai_private_ssh_key)
                .expand(
                    track=tracks_to_process
                )
        ) >> (
            shutdown_vakshuddhi_instance(
                vastai_access_key,
                vakkshuddhi_instance_id
            ).as_teardown(
                setups=[vakkshuddhi_instance_id]
            )
        )
    )


batch_audio_process()
