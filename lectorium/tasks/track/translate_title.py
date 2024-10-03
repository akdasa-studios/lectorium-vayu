from os import environ
from re import findall

from airflow.decorators import task

from lectorium.services import DeepLService

deepl = DeepLService(api_key=environ.get("DEEPL_API_KEY"))


@task(
    task_display_name="Translate Title",
    map_index_template="{{ task.op_kwargs['language'] }}",
)
def translate_title(
    title: str,
    language: str,
) -> dict:
    response_text = deepl.translate(title, language)
    return {
        "title": response_text,
        "language": language,
    }
