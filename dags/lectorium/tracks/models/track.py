from typing import TypedDict


class Track(TypedDict):
    _id: str
    urs: str
    audioNormalizedUrl: str | None
    title: dict[str, str]
    location: str | None
    date: list[int] | None
    author: str
    file_size: int
    duration: int
    references: list[list[str|int]]
    languages: list[dict[str, str]]

