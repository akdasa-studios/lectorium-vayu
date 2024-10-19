from airflow.models import Variable

from lectorium.services import AntropicService


def run_prompt_with_claude(request: str) -> str:
    antropic_service_api = Variable.get("antropic_api_key")
    service = AntropicService(antropic_service_api)
    return service.process(request_user=request)
