# from airflow.decorators import task
# from airflow.models import Variable

# from lectorium.services.couchdb import CouchDbService
# from lectorium.track_inbox import NormalizedValue, TrackInboxInfo


# @task(task_display_name="⬇️ Get Track Inbox Metadata")
# def get_track_inbox_metadata(
#     track_id: str,
# ) -> TrackInboxInfo:

#     # ---------------------------------------------------------------------------- #
#     #                                 Dependencies                                 #
#     # ---------------------------------------------------------------------------- #

#     tracks_inbox_collection_name = Variable.get("tracks_inbox_collection_name")
#     database_connection_string = Variable.get("database_connection_string")
#     couch_db = CouchDbService(database_connection_string)

#     # ---------------------------------------------------------------------------- #
#     #                                     Steps                                    #
#     # ---------------------------------------------------------------------------- #

#     doc = couch_db.find_by_id(tracks_inbox_collection_name, track_id)

#     # ---------------------------------------------------------------------------- #
#     #                                    Output                                    #
#     # ---------------------------------------------------------------------------- #

#     return TrackInboxInfo(
#         title=NormalizedValue(**doc["title"]),
#         file_size=doc["file_size"],
#         duration=doc["duration"],
#         source=doc["source"],
#         location=NormalizedValue(**doc["location"]),
#         author=NormalizedValue(**doc["author"]),
#         date=NormalizedValue(**doc["date"]),
#         references=[NormalizedValue(**ref) for ref in doc["references"]],
#     )
