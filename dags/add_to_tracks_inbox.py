from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email_smtp
from pendulum import duration

import services.aws as aws
import services.couchdb as couchdb
import services.claude as claude
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📨 Add To Tracks Inbox",
    description="Adds new file to the tracks inbox",
    schedule=None, # duration(minutes=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 1,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=2),
        "max_retry_delay": duration(hours=2),
    },
    params={
        "track_id": Param(
            default="",
            description="Track ID to generate transcripts for",
            type="string",
            title="Track ID",
            minLength=24,
            maxLength=24,
        ),
        "path": Param(
            default="",
            description="Path to the file to add to the inbox",
            type="string",
            title="Path",
        ),
        "tracks_source_id": Param(
            default="",
            description="Tracks Source ID",
            title="Tracks Source ID",
        ),
    },
    render_template_as_native_obj=True,
    max_active_runs=10,
)
def add_to_tracks_inbox():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id         = "{{ dag_run.conf['track_id'] }}"
    tracks_source_id = "{{ dag_run.conf['tracks_source_id'] }}"
    path             = "{{ dag_run.conf['path'] }}"

    database_collections = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    app_bucket_name = (
         Variable.get(lectorium.config.VAR_APP_BUCKET_NAME)
    )

    app_bucket_creds: lectorium.config.AppBucketAccessKey = (
        Variable.get(
            lectorium.config.VAR_APP_BUCKET_ACCESS_KEY,
            deserialize_json=True)
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # ------------------------ Get Tracks Source Document ------------------------ #

    tracks_source = (
        couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_sources"],
            document_id=tracks_source_id
        )
    )

    # ----------------------------- Extract Metadata ----------------------------- #

    prompt = (
        lectorium.tracks_inbox.get_extract_meta_prompt(
            language=tracks_source["language"])
    )

    @task(
        task_display_name="⛏️ Extract Metadata From File Name")
    def extract_metadata_from_file_name(
        path: str,
        prompt: str,
    ) -> dict:
        response = claude.actions.execute_prompt(
            user_message_prefix=prompt,
            user_message=path)
        response_tokens = response.split("|")
        get_by_index = lambda index: (
            response_tokens[index].strip()
            if index < len(response_tokens) else None
        )
        return {
            "author": get_by_index(0),
            "title": get_by_index(1),
            "date": get_by_index(2),
            "location": get_by_index(3),
            "reference": get_by_index(4),
        }

    metadata = (
        extract_metadata_from_file_name(path=path, prompt=prompt)
    )

    # ----------------------------- Validate Metadata ---------------------------- #

    with TaskGroup(group_id='normalize_data'):
        normalized_date = lectorium.tracks_inbox.normalize_date(metadata["date"])
        normalized_location = (
            lectorium.tracks_inbox.normalize_location(
                metadata["location"],
                language=tracks_source["language"],
                connection_string=couchdb_connection_string,
                collection_name=database_collections["dictionary"])
        )
        normalized_reference = (
            lectorium.tracks_inbox.normalize_reference(
                metadata["reference"],
                language=tracks_source["language"],
                connection_string=couchdb_connection_string,
                collection_name=database_collections["dictionary"])
        )


    with TaskGroup(group_id='extract_data'):
        file_size = (
            aws.get_file_size(
                credentials=app_bucket_creds,
                bucket_name=app_bucket_name,
                object_key=path)
        )
        audio_duration = (
            aws.get_audio_duration(
                credentials=app_bucket_creds,
                bucket_name=app_bucket_name,
                object_key=path)
        )


    # ---------------------------------------------------------------------------- #
    #                                     Save                                     #
    # ---------------------------------------------------------------------------- #

    track_inbox_document = (
        lectorium.tracks_inbox.prepare_track_inbox_document(
            track_id=track_id,
            source_path=path,
            file_path=path,
            file_size=file_size,
            audio_duration=audio_duration,
            filename_metadata=metadata,
            author_id=tracks_source["author"],
            location_id=normalized_location,
            date=normalized_date,
            reference=normalized_reference)
    )

    saved_document = (
        couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document=track_inbox_document)
    )

    metadata >> [
        normalized_date,
        normalized_location,
        normalized_reference
    ] >> track_inbox_document

    [
        file_size,
        audio_duration
    ] >> track_inbox_document

    track_inbox_document >> saved_document

    # ---------------------------------------------------------------------------- #
    #                                 Notification                                 #
    # ---------------------------------------------------------------------------- #

    @task
    def generate_report(
        path: str,
        metadata: dict,
        normalized_author: str,
        normalized_date: tuple[int, int, int],
        normalized_location: str,
        normalized_reference: list[str | int],
    ):
        cell_style = "padding: 6px;"
        return f"""
        <h2>File Added to Tracks Inbox</h2>

        <code>
        {path}
        </code>

        <table border='1'>
            <tr>
                <td style='{cell_style}'>Author</td>
                <td style='{cell_style}'>{metadata['author']}</td>
                <td style='{cell_style}'>{normalized_author}</td>
            </tr>
            <tr>
                <td style='{cell_style}'>Title</td>
                <td style='{cell_style}'>{metadata['title']}</td>
                <td style='{cell_style}'>{metadata['title']}</td>
            </tr>
            <tr>
                <td style='{cell_style}'>Date</td>
                <td style='{cell_style}'>{metadata['date']}</td>
                <td style='{cell_style}'>{normalized_date}</td>
            </tr>
            <tr>
                <td style='{cell_style}'>Location</td>
                <td style='{cell_style}'>{metadata['location']}</td>
                <td style='{cell_style}'>{normalized_location}</td>
            </tr>
            <tr>
                <td style='{cell_style}'>Reference</td>
                <td style='{cell_style}'>{metadata['reference']}</td>
                <td style='{cell_style}'>{normalized_reference}</td>
            </tr>
        </table>
        """

    @task()
    def send_notification(
        subject: str,
        content: str,
    ):
        send_email_smtp(
            to=["alerts@akdasa.studio"], # TODO: Change variable
            conn_id="alert_email",
            subject=subject,
            html_content=content)

    saved_document >> send_notification(
        subject="File Added to Tracks Inbox",
        content=generate_report(
            path=path,
            metadata=metadata,
            normalized_author=tracks_source["author"],
            normalized_date=normalized_date,
            normalized_location=normalized_location,
            normalized_reference=normalized_reference))


add_to_tracks_inbox()
