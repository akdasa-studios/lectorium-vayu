from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

import services.couchdb as couchdb
import lectorium as lectorium


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
            {
                "status": { "$in": [ "ready" ] }
            }
        ]
    }


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # --------------------- Get Documents To Start Processing -------------------- #

    track_inbox_documents = (
        couchdb.find_documents(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            filter=to_be_processed_query
        )
    )

    # --------------------------- Set Processing Status -------------------------- #

    track_inbox_documents_processing = (
        couchdb.patch_document
            .partial(
                data={"status": "processing"})
            .expand(
                document=track_inbox_documents)
    )

    track_inbox_documents_processing_saved = (
        couchdb.save_document
            .partial(
                connection_string=couchdb_connection_string,
                collection=database_collections["tracks_inbox"])
            .expand(
                document=track_inbox_documents_processing)
    )

    # ---------------------------- Run Processing DAG ---------------------------- #

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

    started_dags = (
        run_process_track_dag
            .expand(track=track_inbox_documents_processing_saved)
    )


start_processing_tracks()
