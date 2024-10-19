from re import findall

from airflow.decorators import task

from lectorium.transcripts.lib import (
    run_prompt_with_claude,
    run_prompt_with_ollama,
    translate_transcript_prompts,
)
from lectorium.transcripts.models import TranscriptChunk


@task(
    task_display_name="Translate Transcript Chunk",
    map_index_template="{{ task.op_kwargs['transcript_chunk'].language ~ '::' ~ task.op_kwargs['transcript_chunk'].chunk_index | string }}")
def translate_transcript_chunk(
    transcript_chunk: TranscriptChunk,
    language: str,
    translating_service: str = "claude",
) -> TranscriptChunk:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    services = {
        "claude": run_prompt_with_claude,
        "ollama": run_prompt_with_ollama,
    }

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    translating_service = services[translating_service]
    blocks = transcript_chunk.blocks

    # ---------------- proofread transcript using external service --------------- #

    sentences_original = [
        (idx, block.get("text", "")) for idx, block in enumerate(blocks)
    ]
    chunk_plain_text = " ".join([f"{{{s[0]}}} {s[1]}" for s in sentences_original])
    prompt_request = translate_transcript_prompts[language]
    prompt_final = f"""{prompt_request}\n\n {chunk_plain_text}\n\n"""
    response_text = translating_service(prompt_final)

    # ------------------------------ parse response ------------------------------ #

    pattern = r"\{(\d+)\}\s([^{}]+)"
    matches = findall(pattern, response_text)
    sentences_edited = [
        (int(index), str(sentence).strip()) for index, sentence in matches
    ]

    if len(sentences_edited) != len(sentences_original):
        print("ERROR: Different number of sentences in the original and edited")

    for sentence_edited in sentences_edited:
        blocks[sentence_edited[0]]["text"] = sentence_edited[1]

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return TranscriptChunk(
        chunk_index=transcript_chunk.chunk_index,
        language=language,
        blocks=blocks,
    )
