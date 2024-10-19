import glob

from airflow.decorators import task

from lectorium.models.config import TracksSource


@task(task_display_name='Get New Tracks')
def get_new_tracks_on_source(
    tracks_source: TracksSource
) -> list[TracksSource]:
    """
    # Get New Tracks on Source
    Retruns the list of new tracks on the source.
    """
    if tracks_source["uri"].startswith("file://"):
        folder_path = tracks_source["uri"].replace("file://", "")
        new_tracks = glob.glob(folder_path + "/**/*.mp3", recursive=True)
        return [
            {
                "uri": "file://" + track,
                "source": tracks_source,
            } for track in new_tracks
        ]
