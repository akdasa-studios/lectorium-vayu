from datetime import datetime
from re import split

from airflow.decorators import task
from airflow.models import DagRun

from lectorium.models.metadata import AudioInfo, MetadataFromFileName, TrackMetadata
from lectorium.shared import append_dag_run_note


@task(task_display_name="Get Metadata")
def extract_track_metadata(
    file_size: int,
    audio_info: AudioInfo,
    file_metadata: MetadataFromFileName,
    location_id: str,
    author_id: str,
    reference_id: str,
    dag_run: DagRun,
) -> TrackMetadata:
    # normalize the date
    date_normalized = None
    if file_metadata["date"]:
        try:
            date_normalized = datetime.strptime(file_metadata["date"], "%Y%m%d").date()
        except ValueError:
            if file_metadata["date"]:
                append_dag_run_note(
                    dag_run, f"🔴 Date '{file_metadata['date']}' is incorrect format"
                )

    # reference normalization
    reference_normalized = split(" |\.", file_metadata["reference"])
    reference_normalized[0] = reference_id

    # Verse number normalization (e.g. ["01", "02"] -> [1, 2])
    for i in range(1, len(reference_normalized)):
        try:
            reference_normalized[i] = int(reference_normalized[i])
        except ValueError:
            pass
            # append_dag_run_note(dag_run, f"🟡 Date '{file_metadata["date"]}' is incorect format")
            # append_dag_run_note(dag_run, f"🔴 Data '{}' ")

    return {
        "duration": audio_info["duration"],
        "file_size": file_size,
        "author_id": author_id,
        "location_id": location_id,
        "date": (
            [date_normalized.year, date_normalized.month, date_normalized.day]
            if date_normalized
            else None
        ),
        "title": file_metadata["title"],
        "reference": reference_normalized,
    }
