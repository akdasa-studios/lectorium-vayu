from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService
from lectorium.tracks.actions import load_track_info
from lectorium.tracks.models import TrackInfo


@task(task_display_name="Get Track")
def get_track_info(
    track_id: str,
) -> TrackInfo:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    tracks_collection_name = Variable.get("tracks_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(base_url=database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    track_info = load_track_info(track_id, tracks_collection_name, couch_db)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return track_info