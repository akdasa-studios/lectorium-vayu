from os import environ
from re import findall
from typing import Generator

from airflow.decorators import task

from lectorium.services import DeepLService
from lectorium.services.antropic import AntropicService
from lectorium.services.ollama import OllamaService
from lectorium.services.openai import OpenAIService

deepl = DeepLService(api_key=environ.get("DEEPL_API_KEY"))
antropic = AntropicService(api_key=environ.get("ANTHROPIC_API"))
openai = OpenAIService(environ.get("OPENAI_API_KEY"))


def get_transcript_chunk(
    transcript: dict, sentences: int
) -> Generator[str, None, None]:
    result = ""
    sentences_added = 0
    for idx, block in enumerate(transcript):
        if block["type"] == "sentence":  # isinstance(block, TranscriptSentenceBlock):
            result += f"{{{idx}}} {block['text']}\n"
            sentences_added += 1
        if block["type"] == "paragraph":
            result += f"{{{idx}}} \n"
            sentences_added += 1
        if sentences_added >= sentences:
            yield result, sentences_added
            sentences_added = 0
            result = ""
    yield result, sentences_added


@task(
    task_display_name="Translate Transcript",
    map_index_template="{{ task.op_kwargs['transcript']['language'] }}",
)
def translate_transcript(
    transcript: dict,
) -> dict:
    # if language == "en":
    #     language = "en-us"

    # transcript = transcript["transcript"]

    # print("Translating transcript:")
    # print("transcript:", transcript)
    # print("language:", language)

    # # process the transcript in chunks
    # for chunk, sentences_count in get_transcript_chunk(transcript, 50):
    #     if not chunk:
    #         print("Empty chunk, skipping")
    #         continue
    #     print(f"Sending chunk: {chunk}")

    #     response_text = deepl.translate(chunk, language)

    #     # log the result
    #     print("Translated text:", response_text)

    #     # parse the result
    #     pattern = r"\{(\d+)\}\s([^{}]+)"
    #     matches = findall(pattern, response_text)
    #     edited_sentences_dict = {index: sentence.strip() for index, sentence in matches}

    #     # apply changes to the transcript
    #     for sentence_id, sentence_text in edited_sentences_dict.items():
    #         transcript[int(sentence_id)]["text"] = sentence_text

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    ollama = OllamaService(host="207.102.87.207", port=51462)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    language = transcript["language"]
    transcript = transcript["transcript"]

    prompts = {
        "en": f"""
            It is necessary to edit the transcription of the audio lecture, which may contain errors.
            The text contains quotes from sacred texts in Russian in transliteration: Bhagavad-gita, Srimad Bhagavatam and others, highlight them in italics and add a link.
            Service tags of the type {{0}} must be left unchanged.
            No other text should be entered.""",
        "ru": f"""
            Переведите на русский язык. Если что-то не понятно переведите как есть.
            Служебные метики вида {{0}} необходимо оставить без изменений.
            Никакого другого текста не должно быть внесено.""",
        "sr": f"""
            Преведи на српски. Ако нешто није јасно, преведите како јесте.
            Сервисне ознаке облика {{0}} морају остати непромењене.
            Ниједан други текст не треба да буде укључен.""",
    }

    # process the transcript in chunks
    for chunk, sentences_count in get_transcript_chunk(transcript, 50):
        if not chunk:
            print("Empty chunk, skipping")
            continue

        # send the chunk to process
        print(f"\n\n>> Request: {chunk}")
        response_text = antropic.process(
            request_system=prompts[language], request_user=chunk
        ) + "\n\n"
        print("\n\n<< Response:", response_text)

        # parse the result
        pattern = r"\{(\d+)\}\s([^{}]+)"
        matches = findall(pattern, response_text)
        edited_sentences_dict = {index: sentence.strip() for index, sentence in matches}

        # check if the number of sentences in the response
        # matches the number of sentences in the request
        if len(edited_sentences_dict) != sentences_count:
            raise ValueError(
                f"Number of sentences in the response ({len(edited_sentences_dict)}) "
                f"does not match the number of sentences in the request ({sentences_count})."
            )

        # apply changes to the transcript
        for sentence_id, sentence_text in edited_sentences_dict.items():
            transcript[int(sentence_id)]["text"] = sentence_text

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return {
        "transcript": transcript,
        "language": language,
    }
