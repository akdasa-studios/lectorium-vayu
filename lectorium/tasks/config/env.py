from airflow.decorators import task

from lectorium.models import Environment


@task(task_display_name="Get Config")
def get_env_config() -> Environment:
  return {
    "audio_files_bucket_name": "tests",
    "tracks_inbox_collection_name": "library-inbox-tracks",
    "dictionary_collection_name": "test2",
    "transcripts_collection_name": "test2",
    "tracks_collection_name": "test2",
    "index_collection_name": "test2",
  }
  # return {
  #     "audio_files_bucket_name":     "library",
  #     "dictionary_collection_name":  "library-index-v0001",
  #     "transcripts_collection_name": "library-transcripts-v0001",
  #     "tracks_collection_name":      "library-tracks-v0001",
  #     "index_collection_name":       "library-index-v0001",
  # }
