from re import findall
from os import environ
from typing import Generator
from airflow.decorators import task
from lectorium.services.antropic import AntropicService


antropic = AntropicService(
    api_key=environ.get("ANTHROPIC_API"))


def get_transcript_chunk(
  transcript: dict,
  sentences: int
) -> Generator[str, None, None]:
  result = ""
  sentences_added = 0
  for idx, block in enumerate(transcript):
    if block["type"] == "sentence": #isinstance(block, TranscriptSentenceBlock):
      result += f"{{{idx}}} {block['text']} "
      sentences_added += 1
    if sentences_added >= sentences:
      yield result, sentences_added
      sentences_added = 0
      result = ""
  yield result, sentences_added


@task(
    task_display_name="Proofread Transcript",
    map_index_template="{{ task.op_kwargs['transcript']['language'] }}")
def proofread_transcript(
    transcript: dict,
    metadata: dict,
) -> dict:
    language = transcript["language"]
    transcript = transcript["transcript"]
    count_changes, count_total = 0, 0

    prompts = {
        "en": f"""
            It is necessary to edit the transcription of the audio lecture "{metadata.get('title', '')}", which may contain errors.
            The text contains quotes from sacred texts in Russian in transliteration: Bhagavad-gita, Srimad Bhagavatam and others, highlight them in italics and add a link.
            Service tags of the type {{0}} must be left unchanged.
            No other text should be entered.""",
        "ru": f"""
            Нужно редактировать транскрипцию аудио лекции "{metadata.get('title', '')}", которые могут содержать ошибки.
            Текст содержит цитаты из священных текстов на русском в транслитерации: Бхагавад-гита, Шримад Бхагаватам и других их выдели курсивом и добавь ссылку.
            Служебные метики вида {{0}} необходимо оставить без изменений.
            Никакого другого текста не должно быть внесено.""",
    }

    # process the transcript in chunks
    for chunk, sentences_count in get_transcript_chunk(transcript, 150):
        response_text = antropic.process(
            request_system = prompts[language],
            request_user = chunk
        )

        # parse the result
        pattern = r'\{(\d+)\}\s([^{}]+)'
        matches = findall(pattern, response_text)
        edited_sentences_dict = {
            index: sentence.strip()
            for index, sentence in matches
        }

        # apply changes to the transcript
        count_total += sentences_count
        for sentence_id, sentence_text in edited_sentences_dict.items():
            if transcript[int(sentence_id)]["text"] != sentence_text:
              count_changes += 1
            transcript[int(sentence_id)]["text"] = sentence_text

    return {
        "transcript": transcript,
        "language": language,
    }
