from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from pendulum import duration

from lectorium.transcripts.lib import language_params
from lectorium.transcripts.tasks import (
    get_transcript,
    merge_transcript_chunks,
    notify_transcript_saved,
    save_transcript,
    split_transcript_into_chunks,
    translate_transcript_chunk,
)

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "transcripts"],
    dag_display_name="Transcript // Translate",
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
            **language_params,
        ),
        "language_to_translate_into": Param(
            default="en",
            description="Language to translate transcript into",
            title="Translate Into",
            **language_params,
        ),
        "service": Param(
            default="claude",
            description="Service to use for translating",
            type="string",
            title="Translating Service",
            enum=["claude", "ollama"],
            values_display={
                "claude": "Claude",
                "ollama": "Ollama",
            },
        )
    },
)
def dag_translate_transcript():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id                   = "{{ dag_run.conf['track_id'] }}"
    translating_service        = "{{ dag_run.conf['service'] }}"
    language_to_translate_from = "{{ dag_run.conf['language_to_translate_from'] }}"
    language_to_translate_into = "{{ dag_run.conf['language_to_translate_into'] }}"

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    transcript = get_transcript(track_id, language=language_to_translate_from)
    transcript_chunks =\
        split_transcript_into_chunks(transcript=transcript, chunk_size=150)

    # --------------------------------- Translate -------------------------------- #

    transcript_translated_chunks = (
        translate_transcript_chunk
            .partial(
                translating_service=translating_service,
                language=language_to_translate_into)
            .expand(
                transcript_chunk=transcript_chunks)
    )

    transcript_translated =\
        merge_transcript_chunks(transcript_chunks=transcript_translated_chunks)

    # ----------------------------------- Save ----------------------------------- #

    save_transcript(
        track_id=track_id,
        transcript=transcript_translated)

    # ---------------------------------- Notify ---------------------------------- #

    notify_transcript_saved(
        track_id,
        transcript_translated,
        transcript_translated)


dag_translate_transcript()
