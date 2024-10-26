from typing import Generic, TypedDict, TypeVar

T = TypeVar('T')


class NormalizedValue(TypedDict, Generic[T]):
    original: str
    normalized: T | None

class TrackInbox(TypedDict):
    title: NormalizedValue[str]
    author: NormalizedValue[str]
    file_size: int
    duration: int
    references: list[NormalizedValue[list[str]]]
    location: NormalizedValue[str]
    date: NormalizedValue[list[int]]
    source: str


