from airflow.decorators import task


@task(task_display_name="Select Transcripts to Translate", multiple_outputs=False)
def select_transcripts_to_translate(
    transcripts: list[dict], languages: list[str]
) -> dict:
    languages_from_transcripts = set(
        transcript["language"] for transcript in transcripts
    )
    languages_to_translate = set(languages)

    languages_to_translate_into = languages_to_translate - languages_from_transcripts

    print("Languages from transcripts:", languages_from_transcripts)
    print("Languages to translate:", languages_to_translate)
    print("Languages to translate into:", languages_to_translate_into)

    items = list(transcripts)
    item = items[0]
    return [
        {
            "transcript": item["transcript"],
            "language": lang,
        }
        for lang in languages_to_translate_into
    ]
