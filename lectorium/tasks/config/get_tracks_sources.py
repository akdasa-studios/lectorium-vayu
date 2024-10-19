from airflow.decorators import task

from lectorium.models.config import TracksSource


@task(task_display_name='Get Tracks Sources')
def get_tracks_sources() -> list[TracksSource]:
    """
    # Get Tracks Sources
    Gets the sources of the tracks form the database.
    """
    # TODO: Implement the logic to get the tracks sources from the database
    return [
        TracksSource(
            name="inbox",
            uri="file:///akd-studios/lectorium/modules/services/vayu/__files/inbox"
        ),
    ]
