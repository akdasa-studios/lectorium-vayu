from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Param, Variable
from pendulum import duration

import lectorium.index
import services.couchdb as couchdb
import lectorium as lectorium


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🔎 Index: Update Search Index",
    description="Updates search index for the given track.",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks"],
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
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
            description="Track ID to update search index for",
            type="string",
            title="Track ID",
        ),
        "language": Param(
            default="en",
            description="Language to update search index for",
            title="Language",
            **lectorium.shared.LANGUAGE_PARAMS,
        ),
    },
)
def update_search_index():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id = "{{ dag_run.conf['track_id'] }}"
    language = "{{ dag_run.conf['language'] }}"

    database_collections = (
        Variable.get(
            lectorium.config.LECTORIUM_DATABASE_COLLECTIONS,
            deserialize_json=True
        )
    )

    couchdb_connection_string = (
        Variable.get(lectorium.config.LECTORIUM_DATABASE_CONNECTION_STRING)
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    track_document = (
        couchdb.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections["tracks"],
            document_id=track_id
        )
    )

    words_to_index = (
        lectorium.index.get_words_to_index(
            track=track_document,
            language=language)
    )

    stemmed_words = (
        lectorium.index.get_words_stems(
            words=words_to_index,
            language=language)
    )

    index_documents = (
        couchdb.get_document
            .partial(
                connection_string=couchdb_connection_string,
                collection=database_collections["tracks"],
                return_empty_if_not_found=True)
            .expand(
                document_id=stemmed_words.map(lambda word: f"index::{word}"))
    )

    index_documents_enriched = (
        lectorium.index.update_index_document
            .partial(track_id=track_id)
            .expand(word_index_document=stemmed_words.zip(index_documents))
    )

    saved_documents = (
        couchdb.save_document
            .partial(
                connection_string=couchdb_connection_string,
                collection=database_collections["index"])
            .expand(
                document=index_documents_enriched)
    )

    track_document >> words_to_index >> stemmed_words

update_search_index()
