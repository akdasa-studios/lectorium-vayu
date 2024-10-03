from os import environ
from re import split

from airflow.decorators import task

from lectorium.services import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(task_display_name="Get Reference Id")
def get_reference_id(
    reference: str,
) -> str:
    if reference is None or reference == "":
        print("No reference is provided")
        return None

    reference_tokens = split(" |\.", reference)
    reference_source = reference_tokens[0].upper()

    # TODO: query sources only. add filer by id: "source::"
    # query the database
    query = {"name.en.short": reference_source}  # TODO: add other languages
    db_record = couch_db.find_by_filter("library-dictionary-v0001", query)
    print("query: ", query)
    print("response: ", db_record)

    # check if the location exists
    if len(db_record) == 0:
        raise ValueError(f"Source '{reference_source}' not found")
    if len(db_record) > 1:
        raise ValueError(f"To many sources for '{reference_source}' found")

    # return the location id
    return db_record[0]["_id"].replace("source::", "")
