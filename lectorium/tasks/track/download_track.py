import os
import shutil
from typing import List

from airflow.decorators import task

from lectorium.models.config.tracks_source import TrackSource


@task(task_display_name="Download", pool="download_files_pool")
def download_track(
    track_source: TrackSource,
    save_path: str,
) -> List[str]:
    # source_name = track_source['source']['name']

    if track_source["uri"].startswith("file://"):
        source_file_path = track_source["uri"].replace("file://", "")
        destination_file_path = track_source["uri"].replace(track_source["source"]["uri"], save_path)
        print(f"Moving {source_file_path} to {destination_file_path}")

        # source_directory = track_source["source"]["uri"].replace("file://", "")


        # file_path = track_source["uri"].replace("file://", "")


        # source_file_name = os.path.basename(source_file_path)
        # destination_path = os.path.join(save_path, source_name, source_file_name)

        return shutil.move(source_file_path, destination_file_path)
