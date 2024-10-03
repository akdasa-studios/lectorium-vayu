from typing import TypedDict


class TrackMetadata(TypedDict):
    duration: int
    file_size: int
    author_id: str | None
    location_id: str | None
    date: tuple[int, int, int] | None
    title: str | None
    reference: list[list[str | int]] | None
