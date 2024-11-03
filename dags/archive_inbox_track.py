from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Param, Variable
from pendulum import duration

import services.aws as aws
import services.couchdb as couchdb
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📨 Inbox: Archive Inbox Track",
    description="Archive inbox track",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "inbox"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 1,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=10),
        "max_retry_delay": duration(hours=2),
    },
    params={
        "track_id": Param(
            default="",
            description="Track ID to archive",
            type="string",
            title="Track ID",
            minLength=24,
            maxLength=24,
        ),
    },
    render_template_as_native_obj=True,
    max_active_runs=10,
)
def archive_inbox_track():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id = "{{ dag_run.conf['track_id'] }}"

    database_collections = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    app_bucket_name = (
         Variable.get(lectorium.config.VAR_APP_BUCKET_NAME)
    )

    app_bucket_creds: lectorium.config.AppBucketAccessKey = (
        Variable.get(
            lectorium.config.VAR_APP_BUCKET_ACCESS_KEY,
            deserialize_json=True)
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # ------------------------- Get Track Inbox Document ------------------------- #

    track_inbox_document = (
        couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document_id=track_id
        )
    )

    # ---------------------------- Set Archived Status --------------------------- #

    archived_document = (
        couchdb.patch_document(
            document=track_inbox_document,
            data={"archived": True})
    )

    saved_document = (
        couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document=archived_document)
    )

    # ---------------------------- Delete Source File ---------------------------- #

    deleted_file = (
        aws.delete_file(
            credentials=app_bucket_creds,
            bucket_name=app_bucket_name,
            object_key=track_inbox_document["source"])
    )


    # ---------------------------------------------------------------------------- #
    #                                     Flow                                     #
    # ---------------------------------------------------------------------------- #

    (
        track_inbox_document
        >> archived_document
        >> saved_document
        >> deleted_file
    )

archive_inbox_track()
