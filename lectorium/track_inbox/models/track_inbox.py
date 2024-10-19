from dataclasses import dataclass
from typing import Generic, TypedDict, TypeVar

T = TypeVar('T')


# NOTE: airflow fails then decoding nested complex objects,
#       so we need to use TypedDict instead of dataclass here
#       becaause TranscriptBlock is used as a field in TranscriptChunk
class NormalizedValue(TypedDict, Generic[T]):
    original: str
    normalized: T | None

@dataclass
class TrackInboxInfo:
    title: NormalizedValue[str]
    author: NormalizedValue[str]
    file_size: int
    duration: int
    references: list[NormalizedValue[list[str]]]
    location: NormalizedValue[str]
    date: NormalizedValue[list[int]]
    source: str


