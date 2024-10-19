import json

from airflow.decorators import task
from airflow.models import Variable

from lectorium.models.metadata import TrackMetadata
from lectorium.services.couchdb import CouchDbService


@task(task_display_name="Save Track")
def save_track(
    track_id: str,
    track_metadata: TrackMetadata,
    audio_original_url: str,
    audio_normalized_url: str,
    languages_original: list[str],
    languages_translated: list[str],
    translated_title: list[dict] | None,
) -> str:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    tracks_collection_name = Variable.get("tracks_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #
    # TODO: language in metadata might not be the same as in language[0]

    original_language = languages_original[0]

    # -------------------------- translated transcripts -------------------------- #

    extra_languages = []
    for language in languages_translated:
        extra_languages.append(
            {"language": language, "source": "transcript", "type": "generated"}
        )

    # ----------------------------- translated titles ---------------------------- #

    extra_titles = {}
    for title in translated_title:
        extra_titles[title["language"]] = title["title"]

    # --------------------------------- document --------------------------------- #

    document = {
        "_id": track_id,
        "url": audio_original_url,
        "audioNormalizedUrl": audio_normalized_url,
        "title": {
            original_language: track_metadata["title"]["normalized"],
        }
        | extra_titles,
        "location": track_metadata["location"]["normalized"],
        "date": track_metadata["date"].get("normalized", None),
        "author": track_metadata["author"]["normalized"],
        "file_size": track_metadata["file_size"],
        "duration": track_metadata["duration"],
        "references": [x["normalized"] for x in track_metadata["references"]],
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

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    print(json.dumps(document, indent=2, ensure_ascii=False))
    couch_db.save(tracks_collection_name, document)
