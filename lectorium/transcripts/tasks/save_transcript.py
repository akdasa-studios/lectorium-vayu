from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService
from lectorium.transcripts.models import Transcript


@task(
    task_display_name="Save Transcript",
    map_index_template="{{ task.op_kwargs['transcript'].language }}",
)
def save_transcript(
    track_id: str,
    transcript: Transcript,
):
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    transcripts_collection_name = Variable.get("transcripts_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    doc_id = f"{track_id}::{transcript.language}"
    document = {"_id": doc_id, "text": {"blocks": transcript.blocks}}

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    couch_db.save(transcripts_collection_name, document)
