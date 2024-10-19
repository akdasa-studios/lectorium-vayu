from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Param
from pendulum import duration

from lectorium.transcripts.lib import language_params
from lectorium.transcripts.tasks import (
    extract_transcript,
    merge_transcript_chunks,
    notify_transcript_saved,
    proofread_transcript_chunk,
    save_transcript,
    split_transcript_into_chunks,
)

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "transcripts"],
    dag_display_name="Transcript // Extract",
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=2),
        "max_retry_delay": duration(hours=2),
    },
    render_template_as_native_obj=True,
    params={
        "track_id": Param(
            default="",
            description="Track ID to generate transcripts for",
            type="string",
            title="Track ID",
        ),
        "url": Param(
            default="",
            description="URL to the audio file",
            type="string",
            title="Audio File URL",
        ),
        "language": Param(
            default="en",
            description="Extract transcript in the given language",
            title="Language",
            **language_params,
        ),
        "service": Param(
            default="claude",
            description="Service to use for proofreading",
            type="string",
            title="Proofreading Service",
            enum=["claude", "ollama"],
            values_display={
                "claude": "Claude",
                "ollama": "Ollama",
            },
        ),
        "chunk_size": Param(
            default=150,
            description="Number of blocks in a chunk",
            type="integer",
            title="Chunk Size",
        ),
    },
)
def dag_extract_transcript():
    """
    Extracts transcript for the given track in the given languages.
    """

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id             = "{{ dag_run.conf['track_id'] }}"
    track_audio_file_url = "{{ dag_run.conf['url'] }}"
    language             = "{{ dag_run.conf['language'] }}"
    chunk_size           = "{{ dag_run.conf['chunk_size'] | int }}"
    proofreading_service = "{{ dag_run.conf['service'] }}"


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #


    # ---------------------------- Extract Transcript ---------------------------- #

    transcript_extracted = (
        extract_transcript(
            file_url=track_audio_file_url,
            language=language)
    )

    # ---------------------------- Proofread Transcript -------------------------- #

    transcript_extracted_chunks = (
        split_transcript_into_chunks(
            transcript=transcript_extracted,
            chunk_size=chunk_size)
    )

    transcript_proofread_chunks = (
        proofread_transcript_chunk
            .partial(proofreading_service=proofreading_service)
            .expand(transcript_chunk=transcript_extracted_chunks)
    )

    transcript_proofread = (
        merge_transcript_chunks(
            transcript_chunks=transcript_proofread_chunks)
    )

    # ------------------------------ Save Transcript ----------------------------- #

    (
        save_transcript(
            track_id=track_id,
            transcript=transcript_proofread)
    )

    # ---------------------------------- Notify ---------------------------------- #

    (
        notify_transcript_saved(
            track_id,
            transcript_extracted,
            transcript_proofread)
    )


dag_extract_transcript()
