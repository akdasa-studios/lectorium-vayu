from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService
from lectorium.transcripts.models import Transcript, TranscriptBlock


@task(task_display_name="Get Transcript")
def get_transcript(
    track_id: str,
    language: str,
) -> Transcript:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    transcripts_collection_name = Variable.get("transcripts_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(base_url=database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    track_info = couch_db.find_by_id(
        transcripts_collection_name, f"{track_id}::{language}"
    )

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    if track_info is None:
        raise Exception(
            f"Transcript for track {track_id} and language {language} not found "
            f"in the {transcripts_collection_name} collection on {couch_db.base_url}"
        )

    return Transcript(
        language=language,
        blocks=[TranscriptBlock(**block) for block in track_info["text"]["blocks"]],
    )
