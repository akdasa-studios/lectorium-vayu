proofread_transcript_prompts = {
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

translate_transcript_prompts = {
    "en": """Translate into English""",
    "ru": """Translate into Russian""",
    "sr": """Translate into Serbian""",
    "es": """Translate into Spanish""",
}