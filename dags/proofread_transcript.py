from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Param, Variable
from pendulum import duration

import services as services
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📜 Transcript: Proofread",
    description="Proofreads the extracted transcript for the given track in the given language.",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lectorium", "tracks", "transcripts"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=5),
        "max_retry_delay": duration(hours=2),
    },
    render_template_as_native_obj=True,
    params={
        "track_id": Param(
            default="",
            description="Track ID to proofread transcripts for",
            type="string",
            title="Track ID",
            minLength=24,
            maxLength=24,
        ),
        "language": Param(
            default="en",
            description="Language of the transcript to proofread",
            title="Language",
            **lectorium.shared.LANGUAGE_PARAMS,
        ),
        "chunk_size": Param(
            default=150,
            description="Number of blocks in a chunk",
            type="integer",
            title="Chunk Size",
        ),
    },
)
def proofread_transcript():
    """
    # Proofread Transcript
    """

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id      = "{{ dag_run.conf['track_id'] }}"
    transcript_id = "{{ dag_run.conf['track_id'] ~ '::' ~ dag_run.conf['language'] }}"
    language      = "{{ dag_run.conf['language'] }}"
    chunk_size    = "{{ dag_run.conf['chunk_size'] | int }}"

    transcript_collection = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )['transcripts']
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # ------------------------------ Load Transcript ----------------------------- #

    transcript = (
        services.couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=transcript_collection,
            document_id=transcript_id)
    )

    # ---------------------------- Proofread Transcript -------------------------- #

    transcript_chunks = (
        lectorium.transcripts.split_transcript_into_chunks(
            transcript=transcript,
            chunk_size=chunk_size)
    )

    transcript_plain_chunks = (
        lectorium.transcripts.transcript_chunk_to_plain_text
            .expand(transcript_chunk=transcript_chunks)
    )

    proofread_transcript_prompt = (
        lectorium.transcripts.get_proofread_prompt(language=language)
    )

    transcript_proofread_plain_chunks = (
        services.claude.execute_prompt
            .partial(system_message=proofread_transcript_prompt)
            .expand(user_message=transcript_plain_chunks)
    )

    transcript_proofread_chunks = (
        lectorium.transcripts.plain_text_to_transcript_chunk
            .expand(text=transcript_proofread_plain_chunks)
    )

    transcript_enriched_chunks = (
        lectorium.transcripts.enrich_transcript_chunk
            .partial(find_nearest_sentence=True)
            .expand(transcript_chunks=transcript_chunks.zip(transcript_proofread_chunks))
    )

    transcript_proofread = (
        lectorium.transcripts
            .merge_transcript_chunks(transcript_chunks=transcript_enriched_chunks)
    )

    # ------------------------------ Save Transcript ----------------------------- #

    saved_document = (
        services.couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=transcript_collection,
            document=transcript_proofread,
            document_id=transcript_id)
    )


    notification = (
        lectorium.transcripts.send_transcript_saved_report(
            track_id=track_id,
            language=language,
            transcript_original=transcript,
            transcript_proofread=transcript_proofread)
    )

    saved_document >> notification

proofread_transcript()
