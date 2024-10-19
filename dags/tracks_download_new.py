from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Pool, Variable

from lectorium.tasks.config import get_tracks_sources
from lectorium.tasks.lib import flatten
from lectorium.tasks.track import (
    download_track,
    get_new_tracks_on_source,
    run_tracks_add_to_inbox_dag,
)

# ---------------------------------------------------------------------------- #
#                                    Configs                                   #
# ---------------------------------------------------------------------------- #

# Variable.setdefault(
#     "files_processing_path",
#     "/akd-studios/lectorium/modules/services/vayu/__files/processing/",
# )


# ---------------------------------------------------------------------------- #
#                                     Pools                                    #
# ---------------------------------------------------------------------------- #

download_files_pool = Pool.create_or_update_pool(
    "download_files_pool",
    slots=2,
    description="Download files pool",
    include_deferred=False,
)


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #


@dag(
    schedule=timedelta(hours=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks"],
    dag_display_name="Download New Tracks",
)
def tracks_download_new():
    """
    # Download new tracks

    Downloads new tracks from different sources and moves them to the inbox folder.

    ### Schedule

    - **Frequency**: Every hour.
    - **Catch Up**: False

    ### Tasks

    1. **get_tracks_sources**: Gets list of tracks sources from the database.
    2. **get_new_tracks_on_source**: Checks for new tracks in the source.
    3. **download**: Downloads the new tracks and moves them to the inbox folder.
    4. **run_tracks_add_to_inbox_dag**: Runs the `tracks_add_to_inbox` DAG to
       add the tracks to the inbox.
    """
    processing_folder = "{{ var.value.get('files_processing_path') }}"

    tracks_sources = get_tracks_sources()
    new_track_urls = get_new_tracks_on_source.expand(tracks_source=tracks_sources)
    new_track_urls = flatten(new_track_urls)
    downloaded_tracks = download_track.partial(save_path=processing_folder).expand(
        track_source=new_track_urls
    )

    run_tracks_add_to_inbox_dag.expand(file_path=downloaded_tracks)


tracks_download_new()
