from os import environ
from airflow.decorators import task
from lectorium.services import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(
    task_display_name="Get Author Id")
def get_author_id(
    author_name: str,
) -> str:
    # TODO: add other languages
    # TODO: query authors only. add filer by id: "author::"
    # query the database
    languages = ["en", "ru"]

    for language in languages:
        query = { f"name.{language}.full": author_name }
        location_db_record = couch_db.find_by_filter(
            "library-dictionary-v0001", query)
        print("query: ", query)
        print("response: ", location_db_record)

        if len(location_db_record) == 1:
            return location_db_record[0]["_id"].replace("author::", "")

    # Report error
    raise ValueError(f"Author '{author_name}' not found")
