from airflow.decorators import task

from lectorium.tracks_inbox.models.track_inbox import TrackInbox
from lectorium.tracks.models.track import Track


@task(task_display_name="💾 Prepare Track")
def prepare_track_document(
    track_id: str,
    inbox_track: TrackInbox,
    audio_file_original_url: str,
    audio_file_normalized_url: str,
    languages_in_audio_file: list[str],
    languages_to_translate_into: list[str],
    translated_titles: tuple[str, str],
) -> Track:
    date_to_save = inbox_track["date"].get("normalized", []) or []
    date_to_save = list(filter(None, date_to_save)) or None

    document = {
        "_id": track_id,
        "url": audio_file_original_url,
        "audioNormalizedUrl": audio_file_normalized_url,
        "title": {
            lang: inbox_track["title"]["normalized"]
            for lang in languages_in_audio_file
        } | {
            lang: translated_titles
            for (lang, translated_titles) in translated_titles
        },
        "location": inbox_track["location"]["normalized"],
        "date": date_to_save,
        "author": inbox_track["author"]["normalized"],
        "file_size": inbox_track["file_size"],
        "duration": inbox_track["duration"],
        "references": [r["normalized"] for r in inbox_track["references"]],
        "languages":
            [ # original language in audio file
                {"language": lang, "source": "track", "type": "original"}
                for lang in languages_in_audio_file
            ] + [ # original language in audio file for which transcript is generated
                {"language": lang, "source": "transcript", "type": "generated"}
                for lang in languages_in_audio_file
            ] + [ # translated transcripts
                {"language": lang, "source": "transcript", "type": "generated"}
                for lang in languages_to_translate_into
            ]
    }

    if not document.get("date", None):
        del document["date"]

    if not document.get("location", None):
        del document["location"]

    if not document.get("references", None):
        del document["references"]

    return document