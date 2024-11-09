from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Param, Variable
from pendulum import duration

import services as services
import lectorium as lectorium

from lectorium.config import LECTORIUM_DATABASE_COLLECTIONS, LECTORIUM_DATABASE_CONNECTION_STRING
from lectorium.tracks import Track


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📜 Transcript: Translate",
    description="Translates transcript for the given track in the given languages.",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "transcripts"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=2),
        "max_retry_delay": duration(hours=2),
    },
    params={
        "track_id": Param(
            default="",
            description="Track ID to translate transcripts for",
            type="string",
            title="Track ID",
        ),
        "language_to_translate_from": Param(
            default="en",
            description="Language to translate transcript from",
            title="Translate From",
            **lectorium.shared.LANGUAGE_PARAMS,
        ),
        "language_to_translate_into": Param(
            default="en",
            description="Language to translate transcript into",
            title="Translate Into",
            **lectorium.shared.LANGUAGE_PARAMS,
        ),
    },
)
def translate_transcript():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id                   = "{{ dag_run.conf['track_id'] }}"
    transcript_original_id     = "{{ dag_run.conf['track_id'] ~ '::' ~ dag_run.conf['language_to_translate_from'] }}"
    transcript_translated_id   = "{{ dag_run.conf['track_id'] ~ '::' ~ dag_run.conf['language_to_translate_into'] }}"
    language_to_translate_into = "{{ dag_run.conf['language_to_translate_into'] }}"

    database_collections = (
        Variable.get(LECTORIUM_DATABASE_COLLECTIONS, deserialize_json=True)
    )

    couchdb_connection_string = Variable.get(LECTORIUM_DATABASE_CONNECTION_STRING)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    transcript_original = (
        services.couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["transcripts"],
            document_id=transcript_original_id)
    )

    transcript_original_chunks = (
        lectorium.transcripts.split_transcript_into_chunks(
            transcript=transcript_original, chunk_size=100)
    )

    transcript_original_chunks_plain = (
        lectorium.transcripts.transcript_chunk_to_plain_text
            .expand(transcript_chunk=transcript_original_chunks)
    )

    # --------------------------------- Translate -------------------------------- #

    translate_transcript_prompt = (
        lectorium.transcripts.get_translate_prompt(
            language=language_to_translate_into)
    )

    transcript_translated_chunks_plain = (
        services.claude.execute_prompt
            .partial(
                # model="claude-3-haiku-20240307",
                max_tokens=4096,
                system_message=translate_transcript_prompt,
                user_message_prefix=translate_transcript_prompt,
            )
            .expand(user_message=transcript_original_chunks_plain)
    )

    transcript_translated_chunks = (
        lectorium.transcripts.plain_text_to_transcript_chunk
            .expand(text=transcript_translated_chunks_plain)
    )

    transcript_enriched_chunks = (
        lectorium.transcripts.enrich_transcript_chunk
            .expand(transcript_chunks=transcript_original_chunks.zip(transcript_translated_chunks))
    )

    transcript_translated = (
        lectorium.transcripts
            .merge_transcript_chunks(transcript_chunks=transcript_enriched_chunks)
    )

    # ----------------------------------- Save ----------------------------------- #

    saved_document = (
        services.couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections,
            document=transcript_translated,
            document_id=transcript_translated_id)
    )

    # ---------------------------------------------------------------------------- #
    #                             Update Track Document                            #
    # ---------------------------------------------------------------------------- #

    @task(task_display_name="Update Track Document", retries=2)
    def update_track_document(
        track_id: str,
        language: str,
    ):
        # Get the track document from the database to update the languages field
        track_document: Track = services.couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections['tracks'],
            document_id=track_id)

        # Document not found. It may not have been created yet. Skip the task.
        if track_document is None:
            raise AirflowSkipException("Track document not found.")

        # Check if the language already exists in the languages field
        genetared_transcripts_for_language = filter(
            lambda l:
                l["language"] == language and
                l["source"] == "transcript" and
                l["type"] == "generated",
            track_document["languages"]
        )
        if len(list(genetared_transcripts_for_language)) > 0:
            raise AirflowSkipException("Language already exists in the track document.")

        # Add the language to the languages field in the track document
        # and save the document back to the database
        track_document["languages"].append({
            "language": language,
            "source": "transcript",
            "type": "generated",
        })

        return services.couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections['tracks'],
            document=track_document,
            document_id=track_id
        )

    # ---------------------------------- Notify ---------------------------------- #

    notification = (
        lectorium.transcripts.send_transcript_saved_report(
            track_id=track_id,
            language=language_to_translate_into,
            transcript_original=transcript_original,
            transcript_proofread=transcript_translated)
    )

    saved_document >> update_track_document(track_id, language_to_translate_into)
    saved_document >> notification


translate_transcript()
