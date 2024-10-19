import json

from airflow.decorators import task
from airflow.models import Variable

from lectorium.models.metadata.audio_info import AudioInfo
from lectorium.models.metadata.metadata_from_file_name import MetadataFromFileName
from lectorium.services import CouchDbService


@task(task_display_name="Save Inbox")
def save_track_inbox(
    track_id: str,
    file_path: str,
    file_size: int,
    audio_info: AudioInfo,
    filename_metadata: MetadataFromFileName,
    author_id: str | None,
    location_id: str | None,
    date: tuple[int, int, int] | None,
    reference: list[str | int] | None,
):
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    track_inbox_collection_name = Variable.get("tracks_inbox_collection_name")
    database_connection_string = Variable.get("database_connection_string")
    couch_db = CouchDbService(database_connection_string)
    processing_folder = Variable.get("files_processing_path")

    # ---------------------------------------------------------------------------- #
    #                                    Steps                                     #
    # ---------------------------------------------------------------------------- #

    document = {
        "_id": track_id,
        "source": file_path.removeprefix(processing_folder),
        "file_size": file_size,
        "duration": audio_info["duration"],
        "title": {
            "original": filename_metadata["title"],
            "normalized": filename_metadata["title"],
        },
        "author": {
            "original": filename_metadata["author"],
            "normalized": author_id,
        },
        "location": {
            "original": filename_metadata["location"],
            "normalized": location_id,
        },
        "references": (
            [
                {
                    "original": filename_metadata["reference"],
                    "normalized": reference,
                }
            ]
            if reference or filename_metadata["reference"]
            else []
        ),
        "date": {"original": filename_metadata["date"], "normalized": date},
        "status": "error",
    }

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    print(json.dumps(document, indent=2, ensure_ascii=False))
    couch_db.save(track_inbox_collection_name, document)
