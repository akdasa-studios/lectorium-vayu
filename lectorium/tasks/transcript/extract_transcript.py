from os import environ

from airflow.decorators import task

from lectorium.services.deepgram import DeepgramService

deepgram = DeepgramService(api_key=environ.get("DEEPGRAM_API_KEY"))


@task(
    task_display_name="Generate Transcript",
    map_index_template="{{ task.op_kwargs['language'] }}",
)
def extract_transcript(
    file_url: str,
    language: str,
) -> dict:
    response = deepgram.process(file_url, language)

    # process response
    blocks = []
    paragraphs = response["results"]["channels"][0]["alternatives"][0]["paragraphs"][
        "paragraphs"
    ]
    for paragraph in paragraphs:
        # add sentences blocks
        blocks.extend(
            [
                {
                    "type": "sentence",
                    "text": sentence["text"],
                    "start": sentence["start"],
                    "end": sentence["end"],
                }
                for sentence in paragraph["sentences"]
            ]
        )

        # add paragraph block
        blocks.append({"type": "paragraph"})

    return {
        "transcript": blocks,
        "language": language,
    }
