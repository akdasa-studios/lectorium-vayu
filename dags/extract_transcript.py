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
    dag_display_name="Extract Transcript",
    description="Extracts transcript for the given track in the given languages.",
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
            description="Track ID to generate transcripts for",
            type="string",
            title="Track ID",
            minLength=24,
            maxLength=24,
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
def extract_transcript():
    """
    # Extract Transcript

    Extracts transcript for the given track in the given languages and saves it to
    the database.

    ### Parameters
    1. `track_id`: Track ID to generate transcripts for.
    2. `url`: URL to the audio file.
    3. `language`: Extract transcript in the given language.
    4. `chunk_size`: Number of blocks in a chunk.

    ### Output
    1. Saves the extracted transcript with id `track_id::lang` to the database to
       `lectorium::database::collections[transcripts]` collection using
       `lectorium::database::connection-string` connection.
    """

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id      = "{{ dag_run.conf['track_id'] }}"
    transcript_id = "{{ dag_run.conf['track_id'] ~ '::' ~ dag_run.conf['language'] }}"
    file_url      = "{{ dag_run.conf['url'] }}"
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

    # ---------------------------- Extract Transcript ---------------------------- #

    transcript_extracted = (
        services.deepgram.extract_transcript(url=file_url, language=language)
    )

    # ---------------------------- Proofread Transcript -------------------------- #

    transcript_extracted_chunks = (
        lectorium.transcripts.split_transcript_into_chunks(
            transcript=transcript_extracted,
            chunk_size=chunk_size)
    )

    transcript_extracted_plain_chunks = (
        lectorium.transcripts.transcript_chunk_to_plain_text
            .expand(transcript_chunk=transcript_extracted_chunks)
    )

    proofread_transcript_prompt = (
        lectorium.transcripts.get_proofread_prompt(language=language)
    )

    transcript_proofread_plain_chunks = (
        services.claude.execute_prompt
            .partial(system_message=proofread_transcript_prompt)
            .expand(user_message=transcript_extracted_plain_chunks)
    )

    transcript_proofread_chunks = (
        lectorium.transcripts.plain_text_to_transcript_chunk
            .expand(text=transcript_proofread_plain_chunks)
    )

    transcript_enriched_chunks = (
        lectorium.transcripts.enrich_transcript_chunk
            .partial(find_nearest_sentence=True)
            .expand(transcript_chunks=transcript_extracted_chunks.zip(transcript_proofread_chunks))
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
            transcript_original=transcript_extracted,
            transcript_proofread=transcript_proofread)
    )

    saved_document >> notification

extract_transcript()
