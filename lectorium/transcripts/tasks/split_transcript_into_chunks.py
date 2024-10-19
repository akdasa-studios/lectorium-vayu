from airflow.decorators import task

from lectorium.transcripts.models import Transcript, TranscriptChunk


@task(
    task_display_name="Split Transcripts Into Chunks")
def split_transcript_into_chunks(
    transcript: Transcript,
    chunk_size: int,
) -> list[TranscriptChunk]:
    transcript_chunks: list[TranscriptChunk] = []

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    blocks = transcript.blocks
    chunked_blocks = [
        blocks[i:i + chunk_size]
        for i in range(0, len(blocks), chunk_size)
    ]

    for i, chunk in enumerate(chunked_blocks):
        transcript_chunks.append(
            TranscriptChunk(
                chunk_index=i,
                language=transcript.language,
                blocks=chunk,
            )
        )

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return transcript_chunks
