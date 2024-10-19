from dataclasses import dataclass
from typing import TypedDict


# NOTE: airflow fails then decoding nested complex objects,
#       so we need to use TypedDict instead of dataclass here
#       becaause TranscriptBlock is used as a field in TranscriptChunk
class TranscriptBlock(TypedDict):
    type: str
    start: float | None = None
    end: float | None = None
    text: str | None = None


@dataclass
class Transcript:
    language: str
    blocks: list[TranscriptBlock]


@dataclass
class TranscriptChunk:
    chunk_index: int
    language: str
    blocks: list[TranscriptBlock]
