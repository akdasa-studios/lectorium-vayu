# from __future__ import annotations

# from datetime import datetime, timedelta

# from airflow.decorators import dag

# import lectorium.track_inbox as track_inbox

# # ---------------------------------------------------------------------------- #
# #                                      DAG                                     #
# # ---------------------------------------------------------------------------- #


# @dag(
#     schedule=timedelta(minutes=5),
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
#     tags=["lectorium", "tracks"],
#     dag_display_name="Start Processing Ready Inbox Tracks",
# )
# def start_processing_ready_inbox_tracks():

#     ready_tracks_ids = track_inbox.get_ready_tracks()
#     marked_tracks_ids = track_inbox.mark_as_processing.expand(track_id=ready_tracks_ids)
#     track_inbox.start_processing_track.expand(track_id=marked_tracks_ids)

# start_processing_ready_inbox_tracks()
