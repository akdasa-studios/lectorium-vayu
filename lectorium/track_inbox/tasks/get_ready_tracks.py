from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService


@task(task_display_name="Track Inbox: Get Ready Tracks")
def get_ready_tracks() -> list[str]:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    track_inbox_collection_name = Variable.get("tracks_inbox_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    docs = couch_db.find_by_filter(track_inbox_collection_name, {"status": "ready"})

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return [ doc["_id"] for doc in docs ]
