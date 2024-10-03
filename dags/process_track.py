from __future__ import annotations
from typing import TypedDict

import pendulum

from airflow.models.dag import DagRun
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance
from airflow.utils.session import create_session

from lectorium.models import Environment

from lectorium.tasks.files import (
    archive_file,
    upload_file_to_storage)

from lectorium.tasks.index import (
    generate_data_to_index, generate_search_index,
    save_search_index)

from lectorium.tasks.metadata import (
    extract_metadata_from_file_name,
    extract_audio_info_from_file,
    extract_size_info_from_file,
    extract_track_metadata,
    get_location_id, get_author_id,
    get_reference_id)

from lectorium.tasks.track import (
    check_for_duplicates,
    generate_title,
    generate_track_id,
    process_complete,
    save_track,
    translate_title)

from lectorium.tasks.audio import (
    denoise_audio,
    normalize_audio)

from lectorium.tasks.transcript import (
    extract_transcript,
    proofread_transcript,
    save_transcript,
    select_transcript_to_translate, translate_transcript)

class FileToProcess(TypedDict):
    file_path: str

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lectorium"],
    dag_display_name="Process Track",
)
def process_track():
    @task(task_display_name="Get File to Process")
    def get_file_to_process(
        dag_run: DagRun,
        ti: TaskInstance
    ) -> FileToProcess:
        # with create_session() as session:
        #     ctx = ti.get_template_context(session=session)
        #     dag_id = ctx["dag"].dag_id
        #     run_id = ctx["run_id"]
        #     ti = (
        #         session.query(TaskInstance)
        #         .filter(
        #             TaskInstance.dag_id == dag_id,
        #             TaskInstance.task_id == ti.task_id,
        #             TaskInstance.run_id == run_id,
        #             TaskInstance.map_index == ti.map_index,
        #         )
        #         .one()
        #     )
        #     ti.note = "Here is some note"

        #     session.add(ti)
        file_path = dag_run.conf.get('file_path')
        return FileToProcess(file_path=file_path)

    @task(task_display_name="Get Config")
    def get_env_config() -> Environment:
        return {
            "audio_files_bucket_name":     "tests",
            "dictionary_collection_name":  "test2",
            "transcripts_collection_name": "test2",
            "tracks_collection_name":      "test2",
            "index_collection_name":       "test2",
        }
        # return {
        #     "audio_files_bucket_name":     "library",
        #     "dictionary_collection_name":  "library-index-v0001",
        #     "transcripts_collection_name": "library-transcripts-v0001",
        #     "tracks_collection_name":      "library-tracks-v0001",
        #     "index_collection_name":       "library-index-v0001",
        # }

    @task(task_display_name="Get Source Language")
    def get_source_languages() -> str:
        return ["ru"] # TODO: Implement language detection

    @task(task_display_name="Get Target Languages")
    def get_target_languages() -> list[str]:
        return ["en", "sk"]


    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="config"):
        env         = get_env_config()
        lang_target = get_target_languages()
        lang_source = get_source_languages()
        file        = get_file_to_process()
        track_id    = generate_track_id()


    # ---------------------------------------------------------------------------- #
    #                                   Metadata                                   #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="metadata"):
        with TaskGroup(group_id="extract"):
            file_metadata   = extract_metadata_from_file_name(file["file_path"], "ru") # TODO: lang_source)
            file_audio_info = extract_audio_info_from_file(file["file_path"])
            file_size       = extract_size_info_from_file(file["file_path"])

        with TaskGroup(group_id="validate"):
            reference_id = get_reference_id(reference=file_metadata["reference"])
            location_id  = get_location_id(location_name=file_metadata["location"])
            author_id    = get_author_id(author_name=file_metadata["author"])

        metadata = extract_track_metadata(
            file_size     = file_size,
            audio_info    = file_audio_info,
            file_metadata = file_metadata,
            location_id   = location_id,
            author_id     = author_id,
            reference_id  = reference_id,
        )

    # ---------------------------------------------------------------------------- #
    #                             Check for duplicates                             #
    # ---------------------------------------------------------------------------- #

    no_duplicates_found = check_for_duplicates(
        metadata, env=env,
        success_task_id="audio.normalize_audio",
        failure_task_id="archive_file")

    archived_file = archive_file(file["file_path"])

    no_duplicates_found >> archived_file

    # ---------------------------------------------------------------------------- #
    #                                 Process Audio                                #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="audio") as group_audio:
        audio_normalized = normalize_audio(file["file_path"], track_id)

    no_duplicates_found >> group_audio


    # ---------------------------------------------------------------------------- #
    #                                 Upload Audio                                 #
    # ---------------------------------------------------------------------------- #

    file_audio_url = upload_file_to_storage(audio_normalized, track_id, env)


    # ---------------------------------------------------------------------------- #
    #                                  Transcript                                  #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="transcript") as group_transcript:
        with TaskGroup(group_id="generate"):
            transcript_raw    = extract_transcript\
                                    .partial(file_url=file_audio_url)\
                                    .expand(language=lang_source)
            transcript_edited = proofread_transcript\
                                    .partial(metadata=metadata)\
                                    .expand(transcript=transcript_raw)

        with TaskGroup(group_id="translate"):
            transcript_best = select_transcript_to_translate(transcript_edited)
            transcript_translated = translate_transcript\
                                        .partial(transcript=transcript_best)\
                                        .expand(language=lang_target)

        transcript_all = transcript_translated.concat(transcript_edited)
        save_transcript\
            .partial(track_id=track_id, env=env)\
            .expand(transcript=transcript_all)


    # ---------------------------------------------------------------------------- #
    #                                    Titles                                    #
    # ---------------------------------------------------------------------------- #

    generate_title_task  = generate_title(transcript_best)
    track_titles = translate_title\
        .partial(title = metadata["title"])\
        .expand(language=lang_target)\


    # ---------------------------------------------------------------------------- #
    #                                     Track                                    #
    # ---------------------------------------------------------------------------- #

    save_track(
        track_id         = track_id,
        track_metadata   = metadata,
        url              = file_audio_url,
        language         = lang_source,
        generated_title  = generate_title_task,
        translated_title = track_titles,
        env              = env,
    )


    # ---------------------------------------------------------------------------- #
    #                                 Search Index                                 #
    # ---------------------------------------------------------------------------- #

    with TaskGroup(group_id="search_index") as group_search_index:
        index_raw_data = generate_data_to_index(
            track_metadata    = metadata,
            reference_id      = reference_id,
            generated_title   = generate_title_task,
            translated_titles = track_titles.map(lambda x: x["title"]))

        index_words = generate_search_index(index_raw_data, languages=lang_source) # TODO: index_raw_data may contain multiple languages

        save_search_index\
            .partial(track_id=track_id, env=env)\
            .expand(word=index_words)


    # ---------------------------------------------------------------------------- #
    #                                   Complete                                   #
    # ---------------------------------------------------------------------------- #

    process_complete_task = process_complete()

    track_id >> group_search_index
    group_transcript >> group_search_index
    group_search_index >> process_complete_task


process_track()
