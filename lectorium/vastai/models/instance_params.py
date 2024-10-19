from typing import TypedDict


class VastAiInstanceParams(TypedDict):
    num_gpus: int
    gpu_name: str
    image: str
    disk: int = 16
    extra: str = ""
