from typing import TypedDict


class MetadataFromFileName(TypedDict):
    author: str | None
    location: str | None
    date: str | None
    title: str | None
    reference: str | None
