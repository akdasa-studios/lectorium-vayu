from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService


@task(task_display_name="Normalize Author")
def normalize_author(
    author_name: str,
) -> str | None:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    dictionary_collection_name = Variable.get("dictionary_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # TODO: get database name from environment
    # TODO: add other languages
    # TODO: query authors only. add filer by id: "author::"
    languages = ["en", "ru"]
    author_id = None

    for language in languages:
        query = {f"name.{language}.full": author_name}
        location_db_record = couch_db.find_by_filter(dictionary_collection_name, query)
        print("query: ", query)
        print("response: ", location_db_record)

        if len(location_db_record) == 1:
            author_id = location_db_record[0]["_id"].replace("author::", "")
            break

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return author_id
