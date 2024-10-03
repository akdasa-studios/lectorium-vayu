from os import environ

from airflow.decorators import task

from lectorium.models import Environment
from lectorium.services.couchdb import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(
    task_display_name="Save Search Index",
    map_index_template="{{ task.op_kwargs['word'] }}",
)
def save_search_index(
    track_id: str,
    word: str,
    env: Environment,
) -> str:
    database = env["index_collection_name"]

    document_id = f"index::{word}"

    # TODO: add real database
    # try to find existing index document
    document = couch_db.find_by_id(
        database,
        document_id,
    )

    # create a new index document if it does not exist
    if not document:
        print(f"Creating a new index document: {document_id}")
        document = {
            "_id": document_id,
            "in_title": [],
        }
    else:
        print(f"Updating the index document: {document_id}")

    # add the track to the index
    document["in_title"].append(track_id)

    # save the index document
    couch_db.save(database, document)
