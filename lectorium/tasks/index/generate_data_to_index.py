from airflow.decorators import task

from lectorium.models.metadata.track_metadata import TrackMetadata


@task(
    task_display_name="Collect Data to Index")
def generate_data_to_index(
    track_metadata: TrackMetadata,
    reference_id: str,
    generated_title: str | None,
    translated_titles: list[str] | None,
) -> list[str]:
    title            = track_metadata.get("title", None) or generated_title
    year             = track_metadata["date"][0] if track_metadata["date"] else ""
    reference        = track_metadata["reference"] if track_metadata["reference"] else []
    reference_number = ".".join(map(str, reference[1:]))

    return (
        (f"{title} {year} {reference_id} {reference_number} ") +
        (" ".join(translated_titles))
    ).lower().strip()
