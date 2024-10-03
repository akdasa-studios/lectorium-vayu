from os import environ
from airflow.decorators import task

from lectorium.services import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))

@task(
    task_display_name="Get Location Id")
def get_location_id(
    location_name: str,
) -> str:
    if location_name is None or location_name == "":
        print("No location name is provided")
        return None

    # TODO: normalize location name in better way
    # normalize location name
    location_name = location_name.replace("ISKCON", "").strip()

    # TODO: query locations only. add filer by id: "location::"
    # query the database

    languages = ["en", "ru"]
    for language in languages:
        query = { f"name.{language}": location_name } # TODO: add other languages
        location_db_record = couch_db.find_by_filter(
            "library-dictionary-v0001", query)
        print("query: ", query)
        print("response: ", location_db_record)

        if len(location_db_record) == 1:
            return location_db_record[0]["_id"].replace("location::", "")

    # Report error
    raise ValueError(f"Location '{location_name}' not found")

