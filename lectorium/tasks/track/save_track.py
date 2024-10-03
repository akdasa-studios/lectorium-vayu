import json
from os import environ

from airflow.decorators import task

from lectorium.models import Environment
from lectorium.models.metadata import TrackMetadata
from lectorium.services.couchdb import CouchDbService

couch_db = CouchDbService(environ.get("DATABASE_URL"))


@task(task_display_name="Save Track")
def save_track(
    track_id: str,
    track_metadata: TrackMetadata,
    url: str,
    language: str,
    generated_title: str | None,
    translated_title: list[dict] | None,
    env: Environment,
) -> str:
    print("Translating title:", list(translated_title))
    original_language = language[
        0
    ]  # TODO: language in metadata might not be the same as in language[0]
    extra_languages = []
    extra_titles = {}
    for title in translated_title:
        extra_languages.append(
            {"language": title["language"], "source": "transcript", "type": "generated"}
        )
        extra_titles[title["language"]] = title["title"]

    document = {
        "_id": track_id,
        "url": url,
        "title": {
            original_language: track_metadata.get("title", None) or generated_title,
        }
        | extra_titles,
        "location": track_metadata["location_id"],
        "date": track_metadata["date"],
        "author": track_metadata["author_id"],
        "file_size": track_metadata["file_size"],
        "duration": track_metadata["duration"],
        "references": [track_metadata["reference"]],
        "languages": [
            {"language": original_language, "source": "track", "type": "original"},
            {
                "language": original_language,
                "source": "transcript",
                "type": "generated",
            },
        ]
        + extra_languages,
    }

    # log
    print(json.dumps(document, indent=2, ensure_ascii=False))

    # savi in database
    couch_db.save(env["tracks_collection_name"], document)
