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
    dag_display_name="📜 Extract Transcript",
    description="Extracts transcript for the given track in the given language.",
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

    ### Output
    1. Saves the extracted transcript with id `track_id::lang` to the database to
       `lectorium::database::collections[transcripts]` collection using
       `lectorium::database::connection-string` connection.
    """

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    file_url      = "{{ dag_run.conf['url'] }}"
    language      = "{{ dag_run.conf['language'] }}"
    transcript_id = "{{ dag_run.conf['track_id'] ~ '::' ~ dag_run.conf['language'] }}"

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

    transcript_extracted = (
        services.deepgram.extract_transcript(url=file_url, language=language)
    )

    saved_document = (
        services.couchdb.save_document(
            connection_string=couchdb_connection_string,
            collection=transcript_collection,
            document=transcript_extracted,
            document_id=transcript_id)
    )

extract_transcript()
