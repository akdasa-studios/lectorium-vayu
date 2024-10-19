from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService
from lectorium.tracks.actions import load_track_info


@task(task_display_name="Get Track Audio File URL")
def get_track_audio_file_url(
    track_id: str,
) -> str:
    """
    Returns the URL of the audio file of the track with the given id.
    """

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

    return track_info.audioNormalizedUrl or track_info.url
