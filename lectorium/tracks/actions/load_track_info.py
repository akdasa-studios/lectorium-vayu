from lectorium.services import CouchDbService
from lectorium.tracks.models import TrackInfo


def load_track_info(
    track_id: str, collecton_name: str, couch_db: CouchDbService
) -> TrackInfo:
    """
    Loads track info from the database by track id and returns
    it as a TrackInfo object. Raises an exception if the track
    is not found.
    """

    document = couch_db.find_by_id(collecton_name, track_id)
    if document is None:
        raise Exception(
            f"Track with id {track_id} not found in the {collecton_name} "
            f"collection on {couch_db.base_url}"
        )

    return TrackInfo(
        title=document.get("title"),
        url=document.get("url"),
        audioNormalizedUrl=document.get("audioNormalizedUrl", None),
    )
