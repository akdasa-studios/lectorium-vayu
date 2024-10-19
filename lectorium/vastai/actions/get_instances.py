from airflow.models import Variable
from vastai import VastAI

from lectorium.vastai.models.instance import VastAiInstance


def get_instances() -> list[VastAiInstance]:
    """
    Returns a list of instances from VastAI and their details.
    """

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)
    result: list[VastAiInstance] = []

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    show_instances_result = vast_sdk.show_instances()
    show_instances_lines = show_instances_result.split("\n")
    show_instances_lines = [x for x in show_instances_lines if x]

    for show_instance_line in show_instances_lines[1:]:
        tokens = list(filter(None, show_instance_line.split(" ")))
        result.append(
            VastAiInstance(
                id=int(tokens[0]),
                status=tokens[2],
                hostname=tokens[9],
                port=int(tokens[10]),
                label=tokens[16],
            )
        )

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return result
