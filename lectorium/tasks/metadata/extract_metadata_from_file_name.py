from os import environ
from airflow.decorators import task

from lectorium.models.metadata import MetadataFromFileName
from lectorium.services import OpenAIService


@task(
    task_display_name="Get Metadata From File Name")
def extract_metadata_from_file_name(
    path: str,
    language: str,
) -> MetadataFromFileName:
    print("Extracting metadata from the file name:", path)

    prompts = {
        "en": """
            Here is a path to the file. Extract information from it and return the result as: <author>|<topic>|<date YYYYMMDD>|<location>|<verse number>.
            Leave the field blank if there is no information about it. Return an empty string if you are unclear at all. Ignore #hashtags.
            Use | as the delimiter. The verse number might be in the following format: BG 2.14, SB 1.2.3, CC Adi 1.2.3, etc.""",
        "ru": """
            Вот путь к файлу. Извлеките информацию из него и верните результат в виде одной строки в формате: <автор>|<тема>|<дата ГГГГММДД>|<место>|<номер стиха>. Никакой дополнительной информации не нужно.
            Оставьте поле пустым, если информации о нем нет. Верните пустую строку, если вам все непонятно. Игнорируйте #хэштеги.
            Используйте | в качестве разделителя.

            <номер стиха> может быть в формате: БГ 2.14, ШБ 1.2.3, ЧЧ Ади 1.2.3 и т. д.
            Если <номер стиха> содержит раздлитель то убрать: "Ш Б 10.10.11" -> "ШБ 10.10.11", "Б.Г. 1.12" -> "БГ 1.12"

            Примеры локаций: Вриндаван, Майапур
            Пример дат: 20050213, 13/02/2005
            Пример имен: Аиндра дас, Ватсала дас, Нитьянанда Чаран дас
            В именах "прабху" заменяй на "дас" и убирай приставки "ЕМ" и "ЕС"
            """,
    }


    # make a request to the OpenAI service
    prompt = f"""{prompts[language]}

            {path}"""
    openai = OpenAIService(environ.get("OPENAI_API_KEY"))
    response_text = openai.process(
        request_user=prompt)

    # process the response
    print("Prompt:", prompt)
    print("Extracted metadata: ", response_text)

    response_tokens = response_text.split("|")
    get_by_index = (
        lambda index: response_tokens[index].strip()
        if index < len(response_tokens)
        else None)

    # return the result
    return {
        "author": get_by_index(0),
        "title": get_by_index(1),
        "date": get_by_index(2),
        "location": get_by_index(3),
        "reference": get_by_index(4),
    }
