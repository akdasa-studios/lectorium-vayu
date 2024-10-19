from os import environ
from re import split

from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(task_display_name="Normalize Reference")
def normalize_reference(
    reference: str,
) -> list[str | int] | None:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    dictionary_collection_name = Variable.get("dictionary_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    if reference is None or reference == "":
        print("No reference is provided")
        return None

    reference_tokens = split(" |\.", reference)
    reference_source = reference_tokens[0].upper()

    # TODO: add other languages
    # TODO: query sources only. add filer by id: "source::"
    # query the database
    languages = ["en", "ru"]
    for language in languages:
        query = {f"name.{language}.short": reference_source}
        db_record = couch_db.find_by_filter(dictionary_collection_name, query)

    # check if the location exists
    if len(db_record) == 0:
        return None
    if len(db_record) > 1:
        return None

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return [
        db_record[0]["_id"].replace("source::", ""),
        *[int(token) if token.isdigit() else token for token in reference_tokens[1:]],
    ]
