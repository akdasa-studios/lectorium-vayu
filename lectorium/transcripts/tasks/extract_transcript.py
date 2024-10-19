from os import environ

from airflow.decorators import task

from lectorium.services.deepgram import DeepgramService
from lectorium.transcripts.models import Transcript, TranscriptBlock


@task(
    task_display_name="Extract Transcript",
    map_index_template="{{ task.op_kwargs['language'] }}")
def extract_transcript(
    file_url: str,
    language: str,
) -> Transcript:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    deepgram = DeepgramService(api_key=environ.get("DEEPGRAM_API_KEY"))

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    response = deepgram.process(file_url, language)

    blocks = []
    first_channel = response["results"]["channels"][0]
    first_alternative = first_channel["alternatives"][0]["paragraphs"]
    paragraphs = first_alternative["paragraphs"]

    for paragraph in paragraphs:
        blocks.extend([
            TranscriptBlock(
                type="sentence",
                text=sentence["text"],
                start=float(sentence["start"]),
                end=float(sentence["end"]),
            )
            for sentence in paragraph["sentences"]
        ])

        blocks.append(TranscriptBlock(type="paragraph"))

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return Transcript(
        blocks=blocks,
        language=language,
    )

