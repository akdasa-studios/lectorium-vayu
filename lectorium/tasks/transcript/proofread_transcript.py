import json
from re import findall

import jellyfish
from airflow.decorators import task

from lectorium.services import OllamaService
from lectorium.tasks.transcript import get_transcript_chunk


@task(
    task_display_name="Proofread Transcript",
    map_index_template="{{ task.op_kwargs['transcript']['language'] }}")
def proofread_transcript(
    transcript: dict,
) -> dict:
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
        "en": """
            Proofread the transcription of the audio lecture, which may contain errors:

            1. Fix any grammar, spelling, or punctuation errors.
            2. Edit each sentence individually.
            3. Each sentence starts form number in curly bracket. Example: {42}. Keep number in curly brackets unchanged, and not change value of the number.
            4. Do not add any new content to the text.
            5. Do not remove any content from the text.
            6. Response should contain all the sentences even if you didn't find any errors.
            7. Do not split or merge sentences or lines.
            8. Each line should remain in the same order and line as in the original text.
            9. Do not add any summary of corrections made.

            Return all text even if you didn't find any errors.""",
        "ru": """
            Проверьте транскрипцию аудиолекции, которая может содержать ошибки:

            1. Исправьте любые грамматические, орфографические или пунктуационные ошибки.
            2. Отредактируйте каждое предложение по отдельности.
            3. Каждое предложение начинается с числа в фигурных скобках. Пример: {42}. Оставьте число в фигурных скобках без изменений и не меняйте значение числа.
            4. Не добавляйте в текст новый контент.
            5. Не удаляйте контент из текста.
            6. Ответ должен содержать все предложения, даже если вы не нашли никаких ошибок.
            7. Не разделяйте и не объединяйте предложения или строки.
            8. Каждая строка должна оставаться в том же порядке и строке, что и в исходном тексте.
            9. Не добавляйте никаких сводок внесенных исправлений.

            Верните весь текст, даже если вы не нашли никаких ошибок.""",
    }

    # --------------------- process the transcript in sentences_original --------------------- #

    for sentences_original in get_transcript_chunk(transcript, 40):
        if not sentences_original:  # skip empty sentences
            print("Empty chunk, skipping")
            continue

        # send the chunk to process
        prompt_chunks = " ".join(
            map(lambda x: f"{{{x[0]}}} {x[1]}", sentences_original)
        )
        prompt_request = prompts[language]
        prompt_final = f"""{prompt_request}\n\n {prompt_chunks}\n\n"""

        response = ollama.generate(prompt=prompt_final, model="gemma2:27b")
        response_text = response["response"] + "\n"

        # parse the result
        pattern = r"\{(\d+)\}\s([^{}]+)"
        matches = findall(pattern, response_text)
        sentences_edited = [
            (int(index), str(sentence).strip()) for index, sentence in matches
        ]

        if len(sentences_edited) != len(sentences_original):
            print("ERROR: Different number of sentences in the original and edited")

            for sentence_edited in sentences_edited:
                closest_original = min(
                    sentences_original,
                    key=lambda x: jellyfish.damerau_levenshtein_distance(
                        sentence_edited[1], x[1]
                    ),
                )

                if sentence_edited[0] != closest_original[0]:
                    print("MOVED >>> ", closest_original)
                    print("MOVED <<< ", sentence_edited)
                elif sentence_edited != closest_original:
                    print("      >>> ", closest_original)
                    print("      <<< ", sentence_edited)
                else:
                    print("      === ", sentence_edited)

                transcript[sentence_edited[0]]["text"] = sentence_edited[1]
        else:
            for idx, sentence_edited in enumerate(sentences_edited):
                closest_original = sentences_original[idx]

                if sentence_edited != closest_original:
                    print("      >>> ", closest_original)
                    print("      <<< ", sentence_edited)
                else:
                    print("      === ", sentence_edited)

                transcript[sentence_edited[0]]["text"] = sentence_edited[1]

    # ---------------------------------------------------------------------------- #
    #                                    Report                                    #
    # ---------------------------------------------------------------------------- #

    print("---------------------")
    print("Proofread Transcript:")
    print("---------------------")
    print(json.dumps(transcript, indent=4, ensure_ascii=False))

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return {
        "transcript": transcript,
        "language": language,
    }
