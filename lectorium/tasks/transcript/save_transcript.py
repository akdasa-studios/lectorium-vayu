from os import environ
from airflow.decorators import task
from lectorium.services import CouchDbService
from lectorium.models import Environment

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(
    task_display_name="Save Transcript",
    map_index_template="{{ task.op_kwargs['transcript']['language'] }}")
def save_transcript(
    track_id: str,
    transcript: dict,
    env: Environment,
) -> str:
    language   = transcript["language"]
    transcript = transcript["transcript"]

    doc_id     = f"{track_id}::{language}"
    print(f"Saving transcript '{doc_id}'")

    # prepare document to save in the database
    document = {
        "_id": doc_id,
        "text": {
            "blocks": transcript
        }
    }

    # save the document in the database
    # TODO: add real database
    couch_db.save(env["transcripts_collection_name"], document)
