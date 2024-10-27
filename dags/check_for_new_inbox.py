from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from pendulum import duration
from cuid2 import cuid_wrapper

import services.aws as aws
import services.couchdb as couchdb
import services.claude as claude
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📨 Inbox: Check For New Tracks",
    description="Checks if there are any new files in the inbox",
    schedule=None, # duration(minutes=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 1,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=2),
        "max_retry_delay": duration(hours=2),
    },
    params={
        "tracks_source_id": Param(
            default="",
            description="Tracks source ID to check for new files",
            type="string",
            title="Tracks Source ID",
        ),
        "max_items_to_add": Param(
            default=10,
            description="Maximum number of items to add to the inbox",
            type="integer",
            title="Max Items To Add",
        ),
    },
    render_template_as_native_obj=True,
    max_active_runs=1,
)
def check_for_new_inbox():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    tracks_source_id = "{{ dag_run.conf['tracks_source_id'] }}"
    max_items_to_add = "{{ dag_run.conf['max_items_to_add'] | int }}"

    database_collection = (
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
            deserialize_json=True
        )
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    tracks_source = (
        couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collection["tracks_sources"],
            document_id=tracks_source_id
        )
    )

    # ----------------------------- Get List of Files ---------------------------- #

    inbox_items = (
        aws.list_objects(
            credentials=app_bucket_creds,
            bucket_name=app_bucket_name,
            prefix=tracks_source["path"])
    )

    # --------------------- Filter Out Already Added to Inbox -------------------- #

    @task(
        task_display_name="🌟 Get New Only",
        multiple_outputs=False)
    def filter_out_existing_inbox_items(
        connection_string: str,
        collection: str,
        inbox_items: list[str],
        max_items_to_add: int,
    ) -> list[str]:
        result = []
        for item in inbox_items:
            documents = couchdb.actions.find_documents(
                connection_string=connection_string,
                collection=collection,
                filter={"source": item})
            if not documents:
                result.append(item)
        return result[:max_items_to_add]

    filtered_inbox_items = (
        filter_out_existing_inbox_items(
            max_items_to_add=max_items_to_add,
            connection_string=couchdb_connection_string,
            collection=database_collection["tracks_inbox"],
            inbox_items=inbox_items)
    )

    # ------------------ Run DAG To Add New File To Tracks Inbox ----------------- #

    @task(
        task_display_name="📜 Add to Tracks Inbox")
    def add_to_tracks_inbox(
        path: str,
        tracks_source_id: str,
        **kwargs,
    ):
        CUID_GENERATOR: Callable[[], str] = cuid_wrapper()
        track_id = CUID_GENERATOR()

        lectorium.shared.actions.run_dag(
            task_id="add_to_tracks_inbox",
            trigger_dag_id="add_to_tracks_inbox",
            wait_for_completion=False,
            reset_dag_run=True,
            dag_run_params={
                "path": path,
                "track_id": track_id,
                "tracks_source_id": tracks_source_id,
            }, **kwargs
        )


    added_tracks = (
        add_to_tracks_inbox
            .partial(tracks_source_id=tracks_source_id)
            .expand(path=filtered_inbox_items)
    )


check_for_new_inbox()
