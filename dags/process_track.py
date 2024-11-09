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
import services.claude as claude

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
    dag_display_name="▶️ Process Track",
    dagrun_timeout=timedelta(minutes=60),
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
    sign_url_timespan = 60 * 60 * 8 # 8 hours

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
    #                              Get Audio File Urls                             #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="✍️ Get Audio File Urls")
    def get_audio_file_urls(
        track_id: str,
        track_source_path: str,
    ) -> dict:
        """Return local and signed urls for audio file."""
        def sign(method: str, url: str):
            return aws.actions.sign_url(
                credentials=app_bucket_creds,
                bucket_name=app_bucket_name,
                object_key=url,
                method=method,
                expiration=sign_url_timespan)

        return {
            "local_original": f"library/audio/original/{track_id}.mp3",
            "local_processed": f"library/audio/normalized/{track_id}.mp3",
            "signed_get_source": sign("get", track_source_path),
            "signed_put_original": sign("put", f"library/audio/original/{track_id}.mp3"),
            "signed_put_processed": sign("put", f"library/audio/normalized/{track_id}.mp3"),
            "signed_get_processed": sign("get", f"library/audio/normalized/{track_id}.mp3"),
        }

    # ---------------------------------------------------------------------------- #
    #                                 Process Audio                                #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="🔊 Process Audio ⤵️")
    def run_process_audio_dag(
        track_id: str,
        paths: dict,
        **kwargs,
    ):
        return lectorium.shared.actions.run_dag(
            task_id="process_audio",
            trigger_dag_id="process_audio",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(track_id, "process_audio"),
            dag_run_params={
                "track_id": track_id,
                "path_source": paths["signed_get_source"],
                "path_original_dest": paths["signed_put_original"],
                "path_processed_dest": paths["signed_put_processed"],
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                              Extract Transcripts                             #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Extract Transcripts ⤵️",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def extract_transcript(
        track_id: str,
        audio_file_url: str,
        language: str,
        **kwargs,
    ):
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
    #                             Translate Transcript                             #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Translate Transcripts ⤵️",
        map_index_template="{{ task.op_kwargs['language_to_translate_into'] }}")
    def translate_transcript(
        track_id: str,
        language_to_translate_from: str,
        language_to_translate_into: str,
        chunk_size: int,
        **kwargs,
    ):
        lectorium.shared.actions.run_dag(
            task_id="translate_transcript",
            trigger_dag_id="translate_transcript",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(
                            track_id, "translate_transcript",
                            [language_to_translate_from, language_to_translate_into]),
            dag_run_params={
                "track_id": track_id,
                "language_to_translate_from": language_to_translate_from,
                "language_to_translate_into": language_to_translate_into,
                "chunk_size": chunk_size,
            }, **kwargs
        )

    # ---------------------------------------------------------------------------- #
    #                                Translate Title                               #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Translate Titles",
        map_index_template="{{ task.op_kwargs['language'] }}",
        retries=3, retry_delay=timedelta(minutes=1))
    def translate_title(
        track_inbox: lectorium.tracks_inbox.TrackInbox,
        language: str,
    ) -> tuple[str, str]:
        response = claude.actions.execute_prompt(
            user_message_prefix=f"Translate into '{language}'. Return only translation, no extra messages: ",
            user_message=track_inbox["title"]["normalized"])
        return (language, response)


    # ---------------------------------------------------------------------------- #
    #                                  Save Track                                  #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="💾 Save Track",
        trigger_rule="none_failed")
    def save_track(
        track_id: str,
        track_inbox: TrackInbox,
        audio_file_paths: dict,
        languages_in_audio_file: list[str],
        languages_to_translate_into: list[str],
        translated_titles: list[tuple[str, str]] | None,
    ):
        track_document = lectorium.tracks.prepare_track_document(
            track_id=track_id,
            inbox_track=track_inbox,
            audio_file_original_url=audio_file_paths["local_original"],
            audio_file_normalized_url=audio_file_paths["local_processed"],
            languages_in_audio_file=languages_in_audio_file,
            languages_to_translate_into=languages_to_translate_into,
            translated_titles=translated_titles)

        return couchdb.actions.save_document(
            connection_string=database_connection_string,
            collection=database_collections["tracks"],
            document=track_document)

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
        ) >> (
            audio_file_paths :=
                get_audio_file_urls(
                    track_id=track_id,
                    track_source_path=track_inbox["source"])
        ) >> (
            run_process_audio_dag(track_id, audio_file_paths)
        ) >> [
            (languages_in_audio_file     := get_languages_in_audio_file()),
            (languages_to_translate_into := get_languages_to_translate_into())
        ] >> (
            extract_transcript
                .partial(
                    track_id=track_id,
                    audio_file_url=audio_file_paths["signed_get_processed"])
                .expand(
                    language=languages_in_audio_file)
        ) >> (
            proofread_transcript
                .partial(
                    track_id=track_id,
                    chunk_size=chunk_size)
                .expand(
                    language=languages_in_audio_file)
        ) >> [
            (
                translate_transcript
                    .partial(
                        track_id=track_id,
                        language_to_translate_from=get_language_to_translate_from(),
                        chunk_size=chunk_size)
                    .expand(
                        language_to_translate_into=languages_to_translate_into)
            ) , (
                translated_titles := translate_title
                    .partial(track_inbox=track_inbox)
                    .expand(language=languages_to_translate_into)
            )
        ] >> (
            saved_document := save_track(
                track_id=track_id,
                track_inbox=track_inbox,
                audio_file_paths=audio_file_paths,
                languages_in_audio_file=languages_in_audio_file,
                languages_to_translate_into=languages_to_translate_into,
                translated_titles=translated_titles)
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
