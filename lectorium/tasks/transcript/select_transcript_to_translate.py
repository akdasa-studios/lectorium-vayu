from airflow.decorators import task


@task(
    task_display_name="Select Transcript to Translate")
def select_transcript_to_translate(
    transcrips: list[dict]
) -> dict:
    # TODO: Implement selection logic
    items = list(transcrips)
    item = items[0]
    print("Got", items)
    print("Selected", item)
    return item
