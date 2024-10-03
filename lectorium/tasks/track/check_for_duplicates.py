from os import environ

from airflow.decorators import task

from lectorium.models import Environment
from lectorium.models.metadata import TrackMetadata
from lectorium.services.couchdb import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task.branch(task_display_name="Check for Duplicates")
def check_for_duplicates(
    track_metadata: TrackMetadata,
    env: Environment,
    success_task_id: str,
    failure_task_id: str,
) -> bool:
    print(f"Checking for duplicates: {track_metadata}")

    response = couch_db.find_by_filter(
        env["tracks_collection_name"],
        {
            "references": {"$eq": track_metadata["reference"]},
            "date": {"$eq": track_metadata["date"]},
            "author": {"$eq": track_metadata["author_id"]},
        },
    )

    for duplicate in response:
        # log duplicate
        print(
            f"Duplicate found: {duplicate['_id']} "
            f"with reference {track_metadata['reference']} "
            f"and date {track_metadata['date']}"
        )
        return failure_task_id

    return success_task_id
