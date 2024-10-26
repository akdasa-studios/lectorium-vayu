import json

from airflow.decorators import task


@task(task_display_name="Prepare Track Inbox")
def prepare_track_inbox_document(
    track_id: str,
    source_path: str,

    file_path: str,
    file_size: int,
    audio_duration: int,
    filename_metadata: dict,

    author_id: str | None,
    location_id: str | None,
    date: tuple[int, int, int] | None,
    reference: list[str | int] | None,
):
    return {
        "_id": track_id,
        "source": source_path,
        "file_size": file_size,
        "duration": audio_duration,
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
        "status": "new",
    }

