from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param, Variable

import services as services
import lectorium as lectorium

from lectorium.config import LECTORIUM_DATABASE_COLLECTIONS, LECTORIUM_DATABASE_CONNECTION_STRING
from lectorium.tracks import Track


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🇷🇸 Track: Translate",
    description="Translates tracks.",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks", "transcripts"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
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
def translate_track():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    track_id                   = "{{ dag_run.conf['track_id'] }}"
    language_to_translate_into = "{{ dag_run.conf['language_to_translate_into'] }}"
    language_to_translate_from = "{{ dag_run.conf['language_to_translate_from'] }}"

    database_collections = (
        Variable.get(LECTORIUM_DATABASE_COLLECTIONS, deserialize_json=True)
    )

    couchdb_connection_string = Variable.get(LECTORIUM_DATABASE_CONNECTION_STRING)


    # ---------------------------------------------------------------------------- #
    #                                    Helpers                                   #
    # ---------------------------------------------------------------------------- #

    def get_dag_run_id(track_id: str, dag_name: str, extra: list[str] = None) -> str:
        current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{track_id}_{dag_name}_{'_'.join(((extra or []) + ['']))}{current_datetime_string}"


    # ---------------------------------------------------------------------------- #
    #                                   Translate                                  #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Translate Transcript ⤵️")
    def run_translate_transcript_dag(
        track_id: str,
        language_to_translate_from: str,
        language_to_translate_into: str,
        **kwargs,
    ) -> str:
        lectorium.shared.actions.run_dag(
            task_id="translate_transcript",
            trigger_dag_id="translate_transcript",
            wait_for_completion=True,
            reset_dag_run=True,
            dag_run_id=get_dag_run_id(track_id, "translate_transcript", [language_to_translate_from, language_to_translate_into]),
            dag_run_params={
                "track_id": track_id,
                "language_to_translate_from": language_to_translate_from,
                "language_to_translate_into": language_to_translate_into,
            }, **kwargs
        )

    @task(
        task_display_name="📜 Translate Title",
        map_index_template="{{ task.op_kwargs['language_to'] }}",
        retries=3, retry_delay=timedelta(minutes=1))
    def translate_title(
        track_id: str,
        language_from: str,
        language_to: str,
    ) -> str:
        track_document: Track = services.couchdb.actions.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections['tracks'],
            document_id=track_id)

        return services.claude.actions.execute_prompt(
            user_message_prefix=f"Translate into '{language_to}'. Return only translation, no extra messages: ",
            user_message=track_document["title"][language_from])


    # ---------------------------------------------------------------------------- #
    #                             Update Track Document                            #
    # ---------------------------------------------------------------------------- #

    @task(
        task_display_name="📜 Update Track Document",
        retries=3, retry_delay=timedelta(minutes=1))
    def update_track_document(
        track_id: str,
        language: str,
        title: str,
    ):
        # Get the track document from the database to update the languages field
        track_document: Track = services.couchdb.actions.get_document(
            connection_string=couchdb_connection_string,
            collection=database_collections['tracks'],
            document_id=track_id)

        # Update the title in the track document
        track_document["title"][language] = title

        # Check if the language already exists in the languages field
        genetared_transcripts_for_language = filter(
            lambda l:
                l["language"] == language and
                l["source"] == "transcript" and
                l["type"] == "generated",
            track_document["languages"]
        )

        if len(list(genetared_transcripts_for_language)) == 0:
            # Add the language to the languages field in the track document
            # and save the document back to the database
            track_document["languages"].append({
                "language": language,
                "source": "transcript",
                "type": "generated",
            })

        return services.couchdb.actions.save_document(
            connection_string=couchdb_connection_string,
            collection=database_collections['tracks'],
            document=track_document,
            document_id=track_id
        )

    # ---------------------------------------------------------------------------- #
    #                                     Flow                                     #
    # ---------------------------------------------------------------------------- #

    (
        [
            (
                run_translate_transcript_dag(
                    track_id=track_id,
                    language_to_translate_from=language_to_translate_from,
                    language_to_translate_into=language_to_translate_into,
                )
            ),
            (
                translated_title := translate_title(
                    track_id, language_to_translate_from,
                    language_to_translate_into)
            )
        ] >> (
            update_track_document(
                track_id=track_id,
                language=language_to_translate_into,
                title=translated_title,
            )
        )
    )

translate_track()
