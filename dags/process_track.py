from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import DagRun, Param, Variable
from airflow.utils.context import Context
from airflow.operators.python import get_current_context

import lectorium as lectorium
import lectorium.tracks_inbox
import services.couchdb as couchdb
import services.aws as aws

from lectorium.tracks import Track
from lectorium.tracks_inbox import TrackInbox
from lectorium.config import (
    VAR_APP_BUCKET_NAME, VAR_APP_BUCKET_ACCESS_KEY, LECTORIUM_DATABASE_COLLECTIONS,
    LECTORIUM_DATABASE_CONNECTION_STRING, BASE_URL, LectoriumDatabaseCollections,
)

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks"],
    dag_display_name="▶️ Track: Process",
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=3,
    default_args={
        "owner": "Advaita Krishna das",
    },
    render_template_as_native_obj=True,
    params={
        "track_id": Param(
            default="",
            description="Track ID to process",
            type="string",
            title="Track ID",
        ),
        "languages_in_audio_file": Param(
            default=["en"],
            description="Languages present in the audio file. First language will be used for translating transcripts into other languages",
            title="Languages",
            **lectorium.shared.LANGUAGES_PARAMS,
        ),
        "languages_to_translate_into": Param(
            default=[],
            description="Languages to translate transcript into",
            title="Translate Into",
            **lectorium.shared.LANGUAGES_PARAMS,
        ),
        "chunk_size": Param(
            default=150,
            description="Number of blocks in a chunk",
            type="integer",
            title="Chunk Size",
        ),
    },
)
def process_track():
    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id   = "{{ dag_run.conf['track_id'] }}"
    chunk_size = "{{ dag_run.conf['chunk_size'] | int }}"

    app_bucket_name = Variable.get(VAR_APP_BUCKET_NAME)
    app_bucket_creds = Variable.get(VAR_APP_BUCKET_ACCESS_KEY, deserialize_json=True)
    database_connection_string = Variable.get(LECTORIUM_DATABASE_CONNECTION_STRING)
    database_collections: LectoriumDatabaseCollections = (
        Variable.get(LECTORIUM_DATABASE_COLLECTIONS, deserialize_json=True)
    )

    # ---------------------------------------------------------------------------- #
    #                                    Helpers                                   #
    # ---------------------------------------------------------------------------- #

    def get_dag_run_id(track_id: str, dag_name: str, extra: list[str] = None) -> str:
        current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{track_id}_{dag_name}_{'_'.join(((extra or []) + ['']))}{current_datetime_string}"

    # ---------------------------------------------------------------------------- #
    #                                    Helpers                                   #
    # ---------------------------------------------------------------------------- #

    def sign_url(method: str, url: str):
        return aws.actions.sign_url(
            credentials=app_bucket_creds,
            bucket_name=app_bucket_name,
            object_key=url,
            method=method.lower(),
            expiration=60*10)

    # ---------------------------------------------------------------------------- #
    #                                   Language                                   #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="🗣️ Languages In Audio File")
    def get_languages_in_audio_file(dag_run: DagRun):
        return dag_run.conf.get("languages_in_audio_file", [])

    @task(task_display_name="🇷🇸 Translate Into")
    def get_languages_to_translate_into(dag_run: DagRun):
        return dag_run.conf.get("languages_to_translate_into", [])

    @task(task_display_name="🇬🇧 Translate From")
    def get_language_to_translate_from(dag_run: DagRun):
        return dag_run.conf.get("languages_in_audio_file", [])[0]

    # ---------------------------------------------------------------------------- #
    #                                Get Track Inbox                               #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📥 Get Track Inbox")
    def get_track_inbox(
        track_id: str
    ) -> TrackInbox:
        """Load track inbox document for track to be created."""
        return couchdb.actions.get_document(
            connection_string=database_connection_string,
            collection=database_collections["tracks_inbox"],
            document_id=track_id
        )

    # ---------------------------------------------------------------------------- #
    #                                   Add Notes                                  #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📝 Add Note")
    def add_note(
        track_inbox: TrackInbox
    ):
        """Add note to the DAG run."""
        base_url = Variable.get(BASE_URL)
        track_id = track_inbox["_id"]

        context: Context = get_current_context()
        dag_run = context['dag_run']
        lectorium.shared.actions.set_dag_run_note(
            dag_run=dag_run,
            note=(
                f"## `{track_inbox["_id"]}`\n\n"
                f"**Title**: {track_inbox["title"]['normalized']}\n\n"
                f"**Source**: {track_inbox["source"]}\n\n\n\n"
                f"- [📥 Inbox]({base_url}/database/_utils/#database/tracks-inbox/{track_id})\n"
                f"- [💾 Track]({base_url}/database/_utils/#database/library-tracks-v0001/{track_id})\n")
        )

    # ---------------------------------------------------------------------------- #
    #                              Extract Transcripts                             #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Extract Transcripts ⤵️",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def extract_transcript(
        track_id: str,
        language: str,
        **kwargs,
    ):
        audio_file_url = sign_url("get", f"library/audio/normalized/{track_id}.mp3")

        lectorium.shared.actions.run_dag(
            task_id="extract_transcript",
            trigger_dag_id="extract_transcript",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(track_id, "extract_transcript", [language]),
            dag_run_params={
                "track_id": track_id,
                "url": audio_file_url,
                "language": language,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                             Proofread Transcript                             #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Proofread Transcripts ⤵️",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def proofread_transcript(
        track_id: str,
        language: str,
        chunk_size: int,
        **kwargs,
    ):
        lectorium.shared.actions.run_dag(
            task_id="proofread_transcript",
            trigger_dag_id="proofread_transcript",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(track_id, "proofread_transcript", [language]),
            dag_run_params={
                "track_id": track_id,
                "language": language,
                "chunk_size": chunk_size,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                                  Save Track                                  #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="💾 Save Track",
        trigger_rule="none_failed")
    def save_track(
        track_inbox: TrackInbox,
        languages_in_audio_file: list[str],
    ):
        track_id = track_inbox["_id"]
        s3_original = f"library/audio/original/{track_id}.mp3"
        s3_processed = f"library/audio/normalized/{track_id}.mp3"

        track_document = lectorium.tracks.prepare_track_document(
            track_id=track_id,
            inbox_track=track_inbox,
            audio_file_original_url=s3_original,
            audio_file_normalized_url=s3_processed,
            languages_in_audio_file=languages_in_audio_file)

        return couchdb.actions.save_document(
            connection_string=database_connection_string,
            collection=database_collections["tracks"],
            document=track_document)

    # ---------------------------------------------------------------------------- #
    #                                Translate Track                               #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Translate Track ⤵️",
        map_index_template="{{ task.op_kwargs['language_to_translate_into'] }}")
    def run_translate_track_dag(
        track_id: str,
        language_to_translate_from: str,
        language_to_translate_into: str,
        **kwargs,
    ) -> tuple[str, str]:
        lectorium.shared.actions.run_dag(
            task_id="translate_track",
            trigger_dag_id="translate_track",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(
                            track_id, "translate_track",
                            [language_to_translate_from, language_to_translate_into]),
            dag_run_params={
                "track_id": track_id,
                "language_to_translate_from": language_to_translate_from,
                "language_to_translate_into": language_to_translate_into,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                                 Update Index                                 #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="🔍 Update Search Index ⤵️",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def update_index(
        track_id: str,
        language: str,
        **kwargs
    ):
        lectorium.shared.actions.run_dag(
            task_id="update_serach_index",
            trigger_dag_id="update_search_index",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(track_id, "update_search_index", [language]),
            dag_run_params={
                "track_id": track_id,
                "language": language,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                              Archive Inbox Track                             #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📦 Archive Inbox Track ⤵️")
    def run_archive_inbox_track_dag(
        track_id: str,
        **kwargs,
    ):
        lectorium.shared.actions.run_dag(
            task_id="archive_inbox_track",
            trigger_dag_id="archive_inbox_track",
            wait_for_completion=True,
            dag_run_id=get_dag_run_id(track_id, "archive_inbox_track"),
            dag_run_params={
                "track_id": track_id,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                                     Done                                     #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="🎉 Update Track Inbox State")
    def teardown_task(
        track_id: str,
        saved_track: Track = None,
    ):
        # Load track inbox document for specified track
        track_inbox: TrackInbox = couchdb.actions.get_document(
            connection_string=database_connection_string,
            collection=database_collections["tracks_inbox"],
            document_id=track_id)

        # Update track inbox document status
        track_inbox["status"] = "done" if saved_track else "error"
        track_inbox["tasks"]["process_track"] = "done"

        # Save updated track inbox document
        couchdb.actions.save_document(
            connection_string=database_connection_string,
            collection=database_collections["tracks_inbox"],
            document=track_inbox)


    # ---------------------------------------------------------------------------- #
    #                                     Flow                                     #
    # ---------------------------------------------------------------------------- #

    (
        (
            track_inbox := get_track_inbox(track_id=track_id)
        ) >> [
            (languages_in_audio_file     := get_languages_in_audio_file()),
            (languages_to_translate_into := get_languages_to_translate_into()),
            (language_to_translate_from  := get_language_to_translate_from()),
        ] >> (
            extract_transcript
                .partial(track_id=track_id)
                .expand(language=languages_in_audio_file)
        ) >> (
            proofread_transcript
                .partial(
                    track_id=track_id,
                    chunk_size=chunk_size)
                .expand(
                    language=languages_in_audio_file)
        ) >> (
            saved_document := save_track(
                track_inbox=track_inbox,
                languages_in_audio_file=languages_in_audio_file)
        ) >> (
            run_translate_track_dag
                .partial(
                    track_id=track_id,
                    language_to_translate_from=language_to_translate_from)
                .expand(
                    language_to_translate_into=languages_to_translate_into)
        ) >> (
            update_index
                .partial(track_id=track_id)
                .expand(language=languages_in_audio_file.concat(languages_to_translate_into))
        ) >> (
            run_archive_inbox_track_dag(track_id)
        ) >> (
            teardown_task(track_id, saved_document).as_teardown()
        )
    )

    (
        add_note(track_inbox=track_inbox)
    )

process_track()
