from airflow.decorators import task
from mutagen.mp3 import MP3

from lectorium.models.metadata import AudioInfo


@task(task_display_name="Get Audio Info")
def extract_audio_info_from_file(path: str) -> AudioInfo:
    audio = MP3(path)
    return {"duration": int(audio.info.length)}
