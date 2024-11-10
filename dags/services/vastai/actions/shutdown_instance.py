from vastai import VastAI

from services.vastai.models.instance import Instance


def shutdown_instance(
    vast_api_key: str,
    instance: Instance,
) -> int:
    vast_sdk = VastAI(api_key=vast_api_key)
    vast_sdk.stop_instance(ID=instance["id"])
