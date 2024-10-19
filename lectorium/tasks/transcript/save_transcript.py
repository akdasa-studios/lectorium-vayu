from os import environ

from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService


@task(
    task_display_name="Save Transcript",
    map_index_template="{{ task.op_kwargs['transcript']['language'] }}",
)
def save_transcript(
    track_id: str,
    transcript: dict,
) -> str:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    transcripts_collection_name = Variable.get("transcripts_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    language = transcript["language"]
    transcript = transcript["transcript"]

    doc_id = f"{track_id}::{language}"
    print(f"Saving transcript '{doc_id}'")

    # prepare document to save in the database
    document = {"_id": doc_id, "text": {"blocks": transcript}}

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    couch_db.save(transcripts_collection_name, document)
