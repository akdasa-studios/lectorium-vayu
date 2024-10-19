from airflow.decorators import task
from airflow.models import Variable

from lectorium.services.couchdb import CouchDbService


@task(task_display_name="Load Track Metadata")
def load_track_metadata(
    track_id: str,
) -> dict:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    tracks_inbox_collection_name = Variable.get("tracks_inbox_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    doc = couch_db.find_by_id(tracks_inbox_collection_name, track_id)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return doc
