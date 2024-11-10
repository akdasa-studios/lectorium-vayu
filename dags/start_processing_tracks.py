from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

import services.couchdb as couchdb
import lectorium as lectorium

from lectorium.tracks_inbox import TrackInbox

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📨 Inbox: Start Processing Tracks",
    description="Starts Processing tracks in 'ready' state",
    schedule="@hourly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "inbox", "tracks"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
    },
    max_active_runs=1,
)
def start_processing_tracks():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    database_collections = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    to_be_processed_query = {
        "$and": [
            {
                "$or": [
                    { "archived": { "$exists": False } },
                    { "archived": { "$eq": "" } }
                ]
            },
            { "status": "ready" },
            { "tasks.process_audio": "done" },
            { "tasks.process_track": { "$exists": False } }
        ]
    }


    # ---------------------------------------------------------------------------- #
    #                             Get Tracks To Process                            #
    # ---------------------------------------------------------------------------- #


    @task(task_display_name="📨 Get Tracks To Process")
    def get_track_inbox_to_be_processed():
        documents: TrackInbox = couchdb.actions.find_documents(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            filter=to_be_processed_query
        )

        if not documents:
            raise AirflowSkipException("No tracks to process")

        for document in documents:
            if "tasks" not in document:
                document["tasks"] = {}
            document["tasks"]["process_track"] = "processing"

        for document in documents:
            couchdb.actions.save_document(
                connection_string=couchdb_connection_string,
                collection=database_collections["tracks_inbox"],
                document=document
            )

        return documents

    # ---------------------------------------------------------------------------- #
    #                             Run Process Track DAG                            #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📦 Start Processing ⤵️",
        map_index_template="{{ task.op_kwargs['track'].get('_id', 'N/A') }}")
    def run_process_track_dag(
        track: lectorium.tracks_inbox.TrackInbox,
        **kwargs
    ):
        current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
        dag_run_id = f"{track['_id']}_process_track_{current_datetime_string}"

        lectorium.shared.actions.run_dag(
            task_id="process_track",
            trigger_dag_id="process_track",
            wait_for_completion=False,
            dag_run_id=dag_run_id,
            dag_run_params={
                "track_id": track["_id"],
                "languages_in_audio_file": track["extract_languages"],
                "languages_to_translate_into": track.get("translate_into", []),
                "chunk_size": 150,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                                     Flow                                     #
    # ---------------------------------------------------------------------------- #

    (
        (
            tracks_to_process := get_track_inbox_to_be_processed()
        ) >> (
            run_process_track_dag.expand(track=tracks_to_process)
        )
    )

start_processing_tracks()
