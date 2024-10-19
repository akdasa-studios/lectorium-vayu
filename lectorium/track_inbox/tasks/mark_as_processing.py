from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService


@task(task_display_name="Track Inbox: Mark As Processing")
def mark_as_processing(track_id: str) -> str:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    track_inbox_collection_name = Variable.get("tracks_inbox_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    doc = couch_db.find_by_id(track_inbox_collection_name, track_id)
    doc["status"] = "processing"
    couch_db.save(track_inbox_collection_name, doc)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return track_id
