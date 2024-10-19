from dataclasses import dataclass


@dataclass
class TrackInfo:
    title: dict[str, str]
    url: str
    audioNormalizedUrl: str | None
