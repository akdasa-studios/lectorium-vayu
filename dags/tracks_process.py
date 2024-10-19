# from __future__ import annotations

# from os import environ

# import pendulum
# from airflow.decorators import dag, task
# from airflow.models import Pool, Variable
# from airflow.utils.task_group import TaskGroup

# import lectorium.ssh as ssh
# import lectorium.vastai as vastai
# from lectorium.tasks.audio import normalize_audio
# from lectorium.tasks.files import upload_file_to_storage
# from lectorium.tasks.index import (
#     generate_data_to_index,
#     generate_search_index,
#     save_search_index,
# )
# from lectorium.tasks.track import (
#     load_track_metadata,
#     process_complete,
#     save_track,
#     translate_title,
# )
# from lectorium.tasks.transcript import (
#     extract_transcript,
#     proofread_transcript,
#     save_transcript,
#     select_transcripts_to_translate,
#     translate_transcript,
# )

# # ---------------------------------------------------------------------------- #
# #                                   Variables                                  #
# # ---------------------------------------------------------------------------- #

# Variable.setdefault(
#     "storage_access_secret_key", environ.get("STORAGE_ACCESS_SECRET_KEY")
# )
# Variable.setdefault("storage_region", environ.get("STORAGE_REGION"))
# Variable.setdefault("storage_endpoint_url", environ.get("STORAGE_ENDPOINT_URL"))
# Variable.setdefault("storage_access_key_id", environ.get("STORAGE_ACCESS_KEY_ID"))
# Variable.setdefault("storage_public_url", environ.get("STORAGE_PUBLIC_URL"))

# # ---------------------------------------------------------------------------- #
# #                                     Pools                                    #
# # ---------------------------------------------------------------------------- #

# normalize_audio_pool = Pool.create_or_update_pool(
#     "normalize_audio_pool",
#     slots=4,
#     description="Normalize audio pool",
#     include_deferred=False,
# )

# # ---------------------------------------------------------------------------- #
# #                                      DAG                                     #
# # ---------------------------------------------------------------------------- #


# @dag(
#     schedule=None,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["lectorium", "tracks"],
#     dag_display_name="Process Track",
#     max_active_runs=2,
# )
# def tracks_process():
#     # ---------------------------------------------------------------------------- #
#     #                                    Config                                    #
#     # ---------------------------------------------------------------------------- #

#     @task
#     def get_original_audio_url(metadata: dict) -> str:
#         path = Variable.get("files_processing_path")
#         return path + metadata["source"]

#     @task
#     def get_processed_audio_url(metadata: dict) -> str:
#         path = Variable.get("files_processing_path")
#         return path + metadata["source"] + ".processed.mp3"

#     track_id = "{{ dag_run.conf['track_id'] }}"
#     languages_original = ["en"]  # "{{ dag_run.conf['languages_original'] }}"
#     languages_dest = ["ru"]  # "{{ dag_run.conf['languages_dest'] }}"
#     transcript_destination = ["en", "ru", "sr"]
#     metadata = load_track_metadata(track_id)
#     audio_original_local_path = get_original_audio_url(
#         metadata
#     )  # metadata["source"] # "{{ dag_run.conf.get('file_path', None) }}"
#     audio_processed_local_path = get_processed_audio_url(metadata)

#     # ---------------------------------------------------------------------------- #
#     #                                 Process Audio                                #
#     # ---------------------------------------------------------------------------- #

#     with TaskGroup(group_id="process_audio"):
#         vastai_instances = vastai.get_instances()
#         vastai_active_instances = vastai.get_active_instances(vastai_instances)
#         vastai_active_instance = vastai.get_balanced_instance(vastai_active_instances, "audio")
#         vastai_ssh_connection = vastai.get_ssh_connection_to_instance(
#             vastai_active_instance["id"]
#         )

#         s1 = ssh.put_file(
#             vastai_ssh_connection,
#             audio_original_local_path,
#             f"/root/{track_id}/in/{track_id}.mp3",
#         )
#         s2 = ssh.run_commands(
#             vastai_ssh_connection,
#             [
#                 f"cd {track_id}/in;"
#                 f"ffmpeg -v quiet -stats -n -i {track_id}.mp3 {track_id}.wav",
#                 f"cd {track_id};" f"resemble-enhance ./in ./out --denoise_only",
#                 f"cd {track_id}/out; ffmpeg -v quiet -stats -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3",
#             ],
#         )
#         audio_normalized_local_path = ssh.get_file(
#             vastai_ssh_connection,
#             f"/root/{track_id}/out/{track_id}.mp3",
#             audio_processed_local_path,
#         )

#         vastai_ssh_connection >> s1 >> s2 >> audio_normalized_local_path

#     # audio_normalized_local_path = normalize_audio(
#     #     audio_original_local_path,
#     #     track_id,
#     #     vakshuddhi_instance_id=Variable.get("vakshuddhi_instance_id"),
#     #     mode="normal",  # TODO: select mode
#     # )

#     # ---------------------------------------------------------------------------- #
#     #                                 Upload Audio                                 #
#     # ---------------------------------------------------------------------------- #

#     audio_original_url = upload_file_to_storage(
#         audio_original_local_path, f"original/{track_id}"
#     )
#     audio_normalized_url = upload_file_to_storage(
#         audio_normalized_local_path, f"normalized/{track_id}"
#     )

#     # ---------------------------------------------------------------------------- #
#     #                                  Transcript                                  #
#     # ---------------------------------------------------------------------------- #

#     with TaskGroup(group_id="transcript"):
#         transcripts_raw = extract_transcript.partial(
#             file_url=audio_normalized_url
#         ).expand(language=languages_original)

#         transcripts_edited = proofread_transcript.expand(transcript=transcripts_raw)

#         transcripts_to_translate = select_transcripts_to_translate(
#             transcripts=transcripts_edited, languages=transcript_destination
#         )
#         transcript_translated = translate_transcript.expand(
#             transcript=transcripts_to_translate,
#         )
#         save_transcript.partial(track_id=track_id).expand(
#             transcript=transcripts_edited.concat(transcript_translated)
#         )

#     # ---------------------------------------------------------------------------- #
#     #                                    Titles                                    #
#     # ---------------------------------------------------------------------------- #

#     track_titles = translate_title.partial(title=metadata["title"]).expand(
#         language=languages_dest
#     )

#     # ---------------------------------------------------------------------------- #
#     #                                     Track                                    #
#     # ---------------------------------------------------------------------------- #

#     save_track(
#         track_id=track_id,
#         track_metadata=metadata,
#         audio_original_url=audio_original_url,
#         audio_normalized_url=audio_normalized_url,
#         languages_original=languages_original,
#         languages_translated=languages_dest,
#         translated_title=track_titles,
#     )

#     # ---------------------------------------------------------------------------- #
#     #                                 Search Index                                 #
#     # ---------------------------------------------------------------------------- #

#     with TaskGroup(group_id="search_index") as group_search_index:
#         index_raw_data = generate_data_to_index(
#             track_metadata=metadata,
#             translated_titles=track_titles.map(lambda x: x["title"]),
#         )

#         index_words = generate_search_index(
#             index_raw_data, languages=languages_original
#         )  # TODO: index_raw_data may contain multiple languages

#         save_search_index.partial(track_id=track_id).expand(word=index_words)

#     # ---------------------------------------------------------------------------- #
#     #                                   Complete                                   #
#     # ---------------------------------------------------------------------------- #

#     process_complete_task = process_complete()

#     # track_id >> group_search_index
#     # group_transcript >> group_search_index
#     # group_search_index >> process_complete_task


# tracks_process()
