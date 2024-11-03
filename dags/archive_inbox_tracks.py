from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import duration

import services.couchdb as couchdb
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📨 Inbox: Archive Inbox Tracks",
    description="Archive inbox track",
    schedule="@hourly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "inbox"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
    },
    max_active_runs=1,
)
def archive_inbox_tracks():
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

    to_be_archived_query = {
        "$and": [
            {
                "$or": [
                    { "archived": { "$exists": False } },
                    { "archived": { "$eq": "" } }
                ]
            },
            {
                "status": { "$in": [ "done", "cancelled" ] }
            }
        ]
    }


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    track_inbox_documents = (
        couchdb.find_documents(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            filter=to_be_archived_query
        )
    )

    @task(
        task_display_name="📦 Archive Inbox Track ⤵️",
        map_index_template="{{ task.op_kwargs['track'].get('_id', 'N/A') }}")
    def run_archive_inbox_track_dag(track: dict, **kwargs):
        lectorium.shared.actions.run_dag(
            task_id="archive_inbox_track",
            trigger_dag_id="archive_inbox_track",
            wait_for_completion=False,
            dag_run_params={
                "track_id": track["_id"],
            }, **kwargs
        )

    run_archive_inbox_track_dag.expand(track=track_inbox_documents)


archive_inbox_tracks()
