from re import findall

import jellyfish
from airflow.decorators import task

from lectorium.transcripts.lib import (
    proofread_transcript_prompts,
    run_prompt_with_claude,
    run_prompt_with_ollama,
)
from lectorium.transcripts.models import TranscriptChunk


@task(
    task_display_name="Proofread Transcript Chunk",
    map_index_template="{{ task.op_kwargs['transcript_chunk'].language ~ '::' ~ task.op_kwargs['transcript_chunk'].chunk_index | string }}",
)
def proofread_transcript_chunk(
    transcript_chunk: TranscriptChunk, proofreading_service: str = "claude"
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

    proofread_service = services[proofreading_service]
    language = transcript_chunk.language
    blocks = transcript_chunk.blocks

    # ---------------- proofread transcript using external service --------------- #

    sentences_original = [
        (idx, block.get("text", "")) for idx, block in enumerate(blocks)
    ]
    chunk_plain_text = " ".join([f"{{{s[0]}}} {s[1]}" for s in sentences_original])
    prompt_request = proofread_transcript_prompts[language]
    prompt_final = f"""{prompt_request}\n\n {chunk_plain_text}\n\n"""
    response_text = proofread_service(prompt_final)

    # ------------------------------ parse response ------------------------------ #

    pattern = r"\{(\d+)\}\s([^{}]+)"
    matches = findall(pattern, response_text)
    sentences_edited = [
        (int(index), str(sentence).strip()) for index, sentence in matches
    ]

    if len(sentences_edited) != len(sentences_original):
        print("ERROR: Different number of sentences in the original and edited")

        for sentence_edited in sentences_edited:
            if sentence_edited[1] == "":
                continue

            closest_original = min(
                sentences_original,
                key=lambda x: jellyfish.damerau_levenshtein_distance(
                    sentence_edited[1], x[1]
                ),
            )

            move_distance = abs(sentence_edited[0] - closest_original[0])
            if  0 < move_distance <= 3:
                print("MOVED >>> ", closest_original)
                print("MOVED <<< ", sentence_edited)
            elif sentence_edited != closest_original:
                print("      >>> ", closest_original)
                print("      <<< ", sentence_edited)
            else:
                print("      === ", sentence_edited)

            blocks[closest_original[0]]["text"] = sentence_edited[1]
    else:
        for idx, sentence_edited in enumerate(sentences_edited):
            closest_original = sentences_original[idx]

            if sentence_edited != closest_original:
                print("      >>> ", closest_original)
                print("      <<< ", sentence_edited)
            else:
                print("      === ", sentence_edited)

            blocks[sentence_edited[0]]["text"] = sentence_edited[1]

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return TranscriptChunk(
        chunk_index=transcript_chunk.chunk_index,
        language=language,
        blocks=blocks,
    )
