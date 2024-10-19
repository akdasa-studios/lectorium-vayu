from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import DagRun, Param
from airflow.utils.edgemodifier import Label
from pendulum import duration

from lectorium.s3.tasks import s3_get_presigned_url
from lectorium.track_inbox import get_track_inbox_metadata
from lectorium.tracks import run_process_audio_dag
from lectorium.tracks.tasks import save_track
from lectorium.transcripts.lib import languages_params
from lectorium.transcripts.tasks import (
    run_extract_transcript_dag,
    run_translate_transcript_dag,
)

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "tracks"],
    dag_display_name="Tracks // Process",
    dagrun_timeout=timedelta(minutes=60),
    orientation="TB",
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=30),
        "max_retry_delay": duration(hours=2),
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
            **languages_params,
        ),
        "languages_to_translate_into": Param(
            default=[],
            description="Languages to translate transcript into",
            title="Translate Into",
            **languages_params,
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
def dag_process_track():

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

    @task(task_display_name="🗂️ Audio File Path", multiple_outputs=True)
    def get_audio_file_path(track_id: str, metadata: dict) -> str:
        return {
            "inbox": metadata.source,
            "original": f"s3://library/audio/original/{track_id}.mp3",
            "normalized": f"s3://library/audio/normalized/{track_id}.mp3",
        }

    chunk_size = "{{ dag_run.conf['chunk_size'] | int }}"
    track_id = "{{ dag_run.conf['track_id'] }}"
    proofreading_service = "{{ dag_run.conf['service'] }}"
    languages_in_audio_file = get_languages_in_audio_file()
    languages_to_translate_into = get_languages_to_translate_into()

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    inbox_track = get_track_inbox_metadata(track_id)
    audio_file_path = get_audio_file_path(track_id, inbox_track)

    # ------------------------------- Process Audio ------------------------------ #

    @task(task_display_name="📦 Process Audio")
    def process_audio(
        track_id: str,
        path_source: str,
        path_original_dest: str,
        path_processed_dest: str,
        **kwargs
    ) -> str:
        run_process_audio_dag(
            track_id=track_id,
            path_source=path_source,
            path_original_dest=path_original_dest,
            path_processed_dest=path_processed_dest,
            **kwargs,
        )
        return path_processed_dest

    processed_audio = process_audio(
        track_id,
        path_source=audio_file_path["inbox"],
        path_original_dest=audio_file_path["original"],
        path_processed_dest=audio_file_path["normalized"],
    )

    # ---------------------------- Extract Transcript ---------------------------- #

    extracted_transcripts = (
        run_extract_transcript_dag
            .partial(
                track_id=track_id,
                audio_file_url=s3_get_presigned_url("lectorium", processed_audio),
                service=proofreading_service,
                chunk_size=chunk_size)
            .expand(
                language=languages_in_audio_file))

    processed_audio >> extracted_transcripts

    # --------------------------- Translate Transcript --------------------------- #

    translated_transcripts = (
        run_translate_transcript_dag
            .partial(
                track_id=track_id,
                language_to_translate_from=get_language_to_translate_from(),
                service=proofreading_service,
                chunk_size=chunk_size)
            .expand(
                language_to_translate_into=languages_to_translate_into))

    extracted_transcripts >> translated_transcripts

    # ----------------------------------- Save ----------------------------------- #

    saved_track = save_track(
        track_id,
        inbox_track,
        audio_file_path["original"],
        audio_file_path["normalized"],
        languages_in_audio_file,
        languages_to_translate_into)

    translated_transcripts >> saved_track

    # ------------------------------- Update Index ------------------------------- #

    @task(task_display_name="🔍 Update Search Index")
    def update_index():
        pass

    updated_index = update_index()

    # ---------------------------------- Notify ---------------------------------- #

    @task(task_display_name="📧 Notify")
    def notify(track_id: str):
        pass

    saved_track >> updated_index >> notify(track_id)



dag_process_track()
