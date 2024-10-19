from __future__ import annotations

from datetime import datetime
from os import environ

from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from lectorium.tasks.metadata import (
    extract_audio_info_from_file,
    extract_metadata_from_file_name,
    extract_size_info_from_file,
    normalize_author,
    normalize_date,
    normalize_location,
    normalize_reference,
)
from lectorium.tasks.track import generate_track_id, save_track_inbox

# ---------------------------------------------------------------------------- #
#                                  Variables                                   #
# ---------------------------------------------------------------------------- #

Variable.setdefault("audio_files_bucket_name", "tests")
Variable.setdefault("tracks_inbox_collection_name", "library-inbox-tracks")
Variable.setdefault("dictionary_collection_name", "library-dictionary-v0001")
Variable.setdefault("transcripts_collection_name", "library-transcripts-v0001")
Variable.setdefault("tracks_collection_name", "library-tracks-v0001")
Variable.setdefault("index_collection_name", "library-index-v0001")
Variable.setdefault("database_connection_string", environ.get("DATABASE_URL", None))


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks"],
    dag_display_name="Add Tracks To Inbox",
)
def tracks_add_to_inbox():
    """
    # Add New Tracks To Inbox

    ### Inputs:
    - **file_path**: Path to the file to process.

    ### Outputs:
    Save information about track to the `tracks_inbox_collection_name`
    collection in the database.

    ### Tasks:

    Extract:
    1. **Extract Size Info From File**: Extract the size information from the
       file in bytes.
    2. **Extract Metadata From File Name**: Extract metadata from the file name
       such as author, location, date, and reference using specified language.
    3. **Extract Audio Info From File**: Extract audio information from the
       file: duration.

    Normalize:
    1. **Normalize Date**: Normalize the date: `YYYYMMDD` -> `[YYYY, MM, DD]`.
    2. **Normalize Author**: Normalize the author: finds the author in the
       database in the `dictionary_collection_name` and returns the author id.
    3. **Normalize Location**: Normalize the location: finds the location in the
       database in the `dictionary_collection_name` and returns the location id.
    4. **Normalize Reference**: Normalize the reference: finds the reference in
       the database in the `dictionary_collection_name` and returns the
       reference in `[source_id, verse_number, verse_number]` format.

    Output:
    1. **Save Track Inbox**: Save the track information to the
       `tracks_inbox_collection_name` collection in the database.
    """

    # ---------------------------------------------------------------------------- #
    #                                     Input                                    #
    # ---------------------------------------------------------------------------- #

    file = "{{ dag_run.conf['file_path'] }}"

    # ---------------------------------------------------------------------------- #
    #                        Extract And Normalize Metadata                        #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="metadata"):

        with TaskGroup(group_id="form_file_name"):
            file_metadata = extract_metadata_from_file_name(file, "ru")
            date = normalize_date(date=file_metadata["date"])
            author_id = normalize_author(author_name=file_metadata["author"])
            location_id = normalize_location(location_name=file_metadata["location"])
            reference_id = normalize_reference(reference=file_metadata["reference"])

        with TaskGroup(group_id="form_file"):
            file_size = extract_size_info_from_file(file)
            file_audio_info = extract_audio_info_from_file(file)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    save_track_inbox(
        track_id=generate_track_id(),
        file_path=file,
        file_size=file_size,
        audio_info=file_audio_info,
        filename_metadata=file_metadata,
        author_id=author_id,
        location_id=location_id,
        date=date,
        reference=reference_id,
    )


tracks_add_to_inbox()
