from airflow.decorators import task
from airflow.models import Variable
from deepgram import DeepgramClient, PrerecordedOptions

from services.deepgram.config.variables import DEEPGRAM_ACCESS_KEY
from services.deepgram.models.transcript import Transcript, TranscriptBlock


@task(
    task_display_name="📝 Deepgram :: Extract Transcript",
    map_index_template="{{ task.op_kwargs['language'] }}",
)
def extract_transcript(
    url: str,
    language: str,
    model: str = "nova-2",
    smart_format: bool = True,
    paragraphs: bool = True,
) -> Transcript:
    """
    # Deepgram :: Extract Transcript

    Extracts transcript from the given audio file URL.
    """

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    deepgram_access_key = Variable.get(DEEPGRAM_ACCESS_KEY)
    client = DeepgramClient(deepgram_access_key)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #


    # ----------------------------- Extract Transcript---------------------------- #

    client = client.listen.prerecorded.v("1")
    response = client.transcribe_url(
        { "url": url },
        PrerecordedOptions(
            punctuate=True,
            model=model,
            language=language,
            smart_format=smart_format,
            paragraphs=paragraphs,
        ),
    )

    # ------------------------------- Parse Response ----------------------------- #

    blocks = []
    first_channel = response["results"]["channels"][0]
    first_alternative = first_channel["alternatives"][0]["paragraphs"]
    paragraphs = first_alternative["paragraphs"]

    for paragraph in paragraphs:
        blocks.extend(
            [
                TranscriptBlock(
                    type="sentence",
                    text=sentence["text"],
                    start=float(sentence["start"]),
                    end=float(sentence["end"]),
                )
                for sentence in paragraph["sentences"]
            ]
        )

        blocks.append(TranscriptBlock(type="paragraph"))

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return Transcript(
        blocks=blocks,
        language=language,
    )
