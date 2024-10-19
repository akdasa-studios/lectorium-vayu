from difflib import SequenceMatcher

from airflow.decorators import task
from airflow.utils.email import send_email_smtp

from lectorium.transcripts.models import Transcript


@task(
    task_display_name="Notify Transcripts Saved")
def notify_transcript_saved(
    track_id: str,
    transcript_original: Transcript,
    transcript_profread: Transcript,
):
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    def diff_strings(a: str, b: str) -> str:
        output = []
        matcher = SequenceMatcher(None, a, b)
        green = '<span style="color: green">'
        red = '<span style="color: red">'
        endgreen = '</span>'
        endred = '</span>'

        for opcode, a0, a1, b0, b1 in matcher.get_opcodes():
            if opcode == 'equal':
                output.append(a[a0:a1])
            elif opcode == 'insert':
                output.append(f'{green}{b[b0:b1]}{endgreen}')
            elif opcode == 'delete':
                output.append(f'{red}{a[a0:a1]}{endred}')
            elif opcode == 'replace':
                output.append(f'{green}{b[b0:b1]}{endgreen}')
                output.append(f'{red}{a[a0:a1]}{endred}')
        return ''.join(output)

    def get_raw_text(transcript: Transcript) -> str:
        raw_text = ""
        for idx, block in enumerate(transcript.blocks):
            raw_text += f"{{{idx}}} {block.get('text', '')}\n"
        return raw_text


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    language = transcript_original.language

    diff = diff_strings(
        get_raw_text(transcript_original),
        get_raw_text(transcript_profread),
    )
    send_email_smtp(
        to=["alerts@akdasa.studio"],
        conn_id="alert_email",
        subject="New transcript saved",
        html_content=f"""
        <h2>Transcript saved</h2>
            New transcript has been saved for track
            <code>{track_id}</code> on <code>{language}</code> language.
            <br><br>

        <h3>Transcript text</h3>
            {diff}
        """,
    )






