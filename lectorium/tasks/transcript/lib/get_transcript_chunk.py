from typing import Generator


def get_transcript_chunk(
    transcript: dict, sentences: int
) -> Generator[list[tuple[int, str]], None, None]:
    result: list[tuple[int, str]] = []
    sentences_added = 0

    for idx, block in enumerate(transcript):
        if block["type"] == "sentence":
            result.append((idx, block["text"]))
            sentences_added += 1
        if sentences_added >= sentences:
            yield result
            sentences_added = 0
            result = []
    yield result
