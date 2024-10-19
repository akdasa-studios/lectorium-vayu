from airflow.decorators import task
from airflow.models import Variable

from lectorium.services.couchdb import CouchDbService


@task(
    task_display_name="Save Search Index",
    map_index_template="{{ task.op_kwargs['word'] }}",
)
def save_search_index(
    track_id: str,
    word: str,
) -> str:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    index_collection_name = Variable.get("index_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    document_id = f"index::{word}"

    # TODO: add real database
    # try to find existing index document
    document = couch_db.find_by_id(
        index_collection_name,
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

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    couch_db.save(index_collection_name, document)
