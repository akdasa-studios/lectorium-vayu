from typing import TypedDict


class TracksSource(TypedDict):
    name: str
    uri: str

class TrackSource(TypedDict):
    uri: str
    source: TracksSource

