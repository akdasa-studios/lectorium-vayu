from airflow.decorators import task

from lectorium.models.metadata.track_metadata import TrackMetadata


@task(task_display_name="Collect Data to Index")
def generate_data_to_index(
    track_metadata: TrackMetadata,
    translated_titles: list[str] | None,
) -> list[str]:
    title = track_metadata.get("title", {}).get("normalized", "")
    # year = track_metadata.get("date", {}).get if track_metadata["date"] else ""
    reference = track_metadata.get("reference", {}).get("normalized", [])
    reference_number = ".".join(map(str, reference[1:]))

    return (
        (
            (f"{title} {reference[0] if reference else ""} {reference_number} ")
            + (" ".join(translated_titles))
        )
        .lower()
        .strip()
    )
