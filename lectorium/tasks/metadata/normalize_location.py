from os import environ

from airflow.decorators import task
from airflow.models import Variable

from lectorium.services import CouchDbService


@task(task_display_name="Normalize Location")
def normalize_location(
    location_name: str,
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

    location_id = None

    if location_name is None or location_name == "":
        print("No location name is provided")
        return None

    # -------------------------- Normalize Location Name ------------------------- #
    # TODO: normalize location name in better way

    location_name = location_name.replace("ISKCON", "").strip()

    # --------------------- Find the location in the database -------------------- #
    # TODO: query locations only. add filer by id: "location::"

    languages = ["en", "ru"]
    for language in languages:
        query = {f"name.{language}": location_name}  # TODO: add other languages
        location_db_record = couch_db.find_by_filter(dictionary_collection_name, query)
        print("query: ", query)
        print("response: ", location_db_record)

        if len(location_db_record) == 1:
            location_id = location_db_record[0]["_id"].replace("location::", "")
            break

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return location_id
