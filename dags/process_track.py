from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task, teardown
from airflow.models import DagRun, Param, Variable, TaskInstance
from airflow.utils.context import Context
from pendulum import duration

import lectorium as lectorium
import lectorium.tracks_inbox
import services.couchdb as couchdb
import services.aws as aws
import services.claude as claude

from lectorium.tracks import Track
from lectorium.tracks_inbox import TrackInbox

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
        # "retries": 3,
        # "retry_exponential_backoff": True,
        # "retry_delay": duration(seconds=30),
        # "max_retry_delay": duration(hours=2),
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
    pass

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
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

    track_id                     = "{{ dag_run.conf['track_id'] }}"
    chunk_size                   = "{{ dag_run.conf['chunk_size'] | int }}"
    path_to_original_audio_file  = f"library/audio/original/{track_id}.mp3"
    path_to_processed_audio_file = f"library/audio/normalized/{track_id}.mp3"
    languages_in_audio_file      = get_languages_in_audio_file()
    languages_to_translate_into  = get_languages_to_translate_into()

    app_bucket_name = (
         Variable.get(lectorium.config.VAR_APP_BUCKET_NAME)
    )

    app_bucket_creds: lectorium.config.AppBucketAccessKey = (
        Variable.get(
            lectorium.config.VAR_APP_BUCKET_ACCESS_KEY,
            deserialize_json=True
        )
    )

    database_collections = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    sign_url_timespan = 60 * 60 * 8 # 8 hours

    # ---------------------------------------------------------------------------- #
    #                                Get Track Inbox                               #
    # ---------------------------------------------------------------------------- #

    track_inbox = (
        couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document_id=track_id
        )
    )

    @task(
        task_display_name="📝 Add Note")
    def add_note(
        track_inbox: TrackInbox,
    ):
        context: Context = get_current_context()
        dag_run = context['dag_run']
        lectorium.shared.actions.set_dag_run_note(
            dag_run=dag_run,
            note=f"""
            *Track ID*: `{track_inbox["_id"]}`
            *Title*: {track_inbox["title"]['original']}
            """
        )

    track_inbox >> add_note(track_inbox=track_inbox)

    # ---------------------------------------------------------------------------- #
    #                                   Sign Urls                                  #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="✍️ Sign Urls")
    def sign_urls() -> dict:
        sign = (
            lambda url, method: aws.actions.sign_url(
                credentials=app_bucket_creds,
                bucket_name=app_bucket_name,
                object_key=url,
                method=method,
                expiration=sign_url_timespan)
        )

        return {
            "signed_source_audio_file_url": sign("get", track_inbox["source"]),
            "signed_upload_original_audio_file_url": sign("put", path_to_original_audio_file),
            "signed_upload_processed_audio_file_url": sign("put", path_to_processed_audio_file),
            "signed_download_processed_audio_file_url": sign("get", path_to_processed_audio_file),
        }

    signed_urls = sign_urls()

    # ---------------------------------------------------------------------------- #
    #                                 Process Audio                                #
    # ---------------------------------------------------------------------------- #

    processed_audio = (
        task(
            task_display_name="🔊 Process Audio ⤵️",
        )(
            lectorium.shared.actions.run_dag
        )(
            task_id="process_audio",
            trigger_dag_id="process_audio",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_params={
                "track_id": track_id,
                "path_source": signed_urls["signed_source_audio_file_url"],
                "path_original_dest": signed_urls["signed_upload_original_audio_file_url"],
                "path_processed_dest": signed_urls["signed_upload_processed_audio_file_url"],
            }
        )
    )

    track_inbox >> signed_urls >> processed_audio

    # ---------------------------------------------------------------------------- #
    #                              Extract Transcripts                             #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📜 Extract Transcripts ⤵️")
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
            dag_run_params={
                "track_id": track_id,
                "url": audio_file_url,
                "language": language,
            }, **kwargs
        )

    extracted_transcripts = (
        extract_transcript
            .partial(
                track_id=track_id,
                audio_file_url=signed_urls["signed_download_processed_audio_file_url"])
            .expand(
                language=languages_in_audio_file)
    )

    processed_audio >> extracted_transcripts

    # ---------------------------------------------------------------------------- #
    #                             Proofread Transcript                             #
    # ---------------------------------------------------------------------------- #

    @task( # TODO add task map index
        task_display_name="📜 Proofread Transcripts ⤵️")
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
            dag_run_params={
                "track_id": track_id,
                "language": language,
                "chunk_size": chunk_size,
            }, **kwargs
        )

    proofread_transcripts = (
        proofread_transcript
            .partial(
                track_id=track_id,
                chunk_size=chunk_size)
            .expand(
                language=languages_in_audio_file)
    )

    extracted_transcripts >> proofread_transcripts

    # ---------------------------------------------------------------------------- #
    #                             Translate Transcript                             #
    # ---------------------------------------------------------------------------- #

    @task( # TODO add task map index
        task_display_name="📜 Translate Transcripts ⤵️")
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
            dag_run_params={
                "track_id": track_id,
                "language_to_translate_from": language_to_translate_from,
                "language_to_translate_into": language_to_translate_into,
                "chunk_size": chunk_size,
            }, **kwargs
        )

    translated_transcripts = (
        translate_transcript
            .partial(
                track_id=track_id,
                language_to_translate_from=get_language_to_translate_from(),
                chunk_size=chunk_size)
            .expand(
                language_to_translate_into=languages_to_translate_into)
    )

    proofread_transcripts >> translated_transcripts

    # ---------------------------------------------------------------------------- #
    #                                Translate Title                               #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Translate Titles",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def translate_title(
        track_inbox: lectorium.tracks_inbox.TrackInbox,
        language: str,
    ):
        return claude.actions.execute_prompt(
            user_message_prefix=f"Translate into '{language}'. Return only translation, no extra messages: ",
            user_message=track_inbox["title"]["normalized"])


    translated_titles = (
        translate_title
            .partial(track_inbox=track_inbox)
            .expand(language=languages_to_translate_into)
    )

    # ---------------------------------------------------------------------------- #
    #                                  Save Track                                  #
    # ---------------------------------------------------------------------------- #

    track_document = (
        lectorium.tracks.prepare_track_document(
            track_id=track_id,
            inbox_track=track_inbox,
            audio_file_original_url=path_to_original_audio_file,
            audio_file_normalized_url=path_to_processed_audio_file,
            languages_in_audio_file=languages_in_audio_file,
            languages_to_translate_into=languages_to_translate_into,
            translated_titles=languages_to_translate_into.zip(translated_titles))
    )

    saved_document = (
        couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks"],
            document=track_document)
    )

    translated_transcripts >> track_document >> saved_document


    # ------------------------------- Update Index ------------------------------- #

    @task(
        task_display_name="🔍 Update Search Index ⤵️",
        map_index_template="{{ task.op_kwargs['language'] }}")
    def update_index(track_id: str, language: str, **kwargs):
        lectorium.shared.actions.run_dag(
            task_id="update_serach_index",
            trigger_dag_id="update_search_index",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_params={
                "track_id": track_id,
                "language": language,
            }, **kwargs
        )

    updated_index = (
        update_index
            .partial(track_id=track_id)
            .expand(language=languages_in_audio_file.concat(languages_to_translate_into))
    )

    # ---------------------------------------------------------------------------- #
    #                              Archive Inbox Track                             #
    # ---------------------------------------------------------------------------- #

    archived_inbox_track = (
        task(
            task_display_name="📦 Archive Inbox Track ⤵️"
        )(
            lectorium.shared.actions.run_dag
        )(
            task_id="archive_inbox_track",
            trigger_dag_id="archive_inbox_track",
            wait_for_completion=True,
            dag_run_params={
                "track_id": track_id,
            }
        )
    )

    saved_document >> updated_index >> archived_inbox_track

    # ---------------------------------------------------------------------------- #
    #                                     Done                                     #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="📧 Notify")
    def notify(track_id: str):
        pass

    @task(
        task_display_name="🎉 Update Track Inbox State")
    def teardown_task(
        track: Track,
    ):
        # Load track inbox document for specified track
        track_inbox: TrackInbox = couchdb.actions.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document_id=track["_id"])

        # Update track inbox document status
        track_inbox["status"] = "done" if saved_document else "error"

        # Save updated track inbox document
        couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks_inbox"],
            document=track_inbox)


    archived_inbox_track >> notify(track_id) >> teardown_task(saved_document).as_teardown()


process_track()
