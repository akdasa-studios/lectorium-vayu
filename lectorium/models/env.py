from typing import TypedDict


class Environment(TypedDict):
    audio_files_bucket_name: str
    dictionary_collection_name: str
    transcripts_collection_name: str
    tracks_collection_name: str
    tracks_inbox_collection_name: str
    index_collection_name: str
