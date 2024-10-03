from os import environ
from re import findall

from airflow.decorators import task

from lectorium.services import DeepLService

deepl = DeepLService(api_key=environ.get("DEEPL_API_KEY"))


def get_transcript_chunk(
    transcript: dict,
) -> str:
    result = ""
    for idx, block in enumerate(transcript):
        if block["type"] == "sentence":
            result += f"{{{idx}}} {block['text']} "
    return result


@task(
    task_display_name="Translate Transcript",
    map_index_template="{{ task.op_kwargs['language'] }}",
)
def translate_transcript(
    transcript: dict,
    language: str,
) -> dict:
    print("Translating transcript:", transcript)

    transcript = transcript["transcript"]

    print("Translating transcript:")
    print("transcript:", transcript)
    print("language:", language)

    # process the transcript in chunks
    chunk = get_transcript_chunk(transcript)
    response_text = deepl.translate(chunk, language)

    # log the result
    print("Translated text:")
    print(response_text)

    # parse the result
    pattern = r"\{(\d+)\}\s([^{}]+)"
    matches = findall(pattern, response_text)
    edited_sentences_dict = {index: sentence.strip() for index, sentence in matches}

    # apply changes to the transcript
    for sentence_id, sentence_text in edited_sentences_dict.items():
        transcript[int(sentence_id)]["text"] = sentence_text

    return {
        "transcript": transcript,
        "language": language,
    }
