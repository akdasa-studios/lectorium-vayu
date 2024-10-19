from typing import TypedDict


class VastAiInstance(TypedDict):
    id: int
    status: str
    hostname: str
    port: int
    label: str
