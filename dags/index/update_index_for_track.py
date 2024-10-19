from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Param
from pendulum import duration

from lectorium.tracks.tasks import get_track_info

# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium", "index"],
    dag_display_name="Index // Update for Track",
    dagrun_timeout=timedelta(minutes=60),
    orientation="TB",
    default_args={
        "owner": "Advaita Krishna das",
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": duration(seconds=30),
        "max_retry_delay": duration(hours=2),
    },
    render_template_as_native_obj=True,
    params={
        "track_id": Param(
            default="",
            description="Track ID to process",
            type="string",
            title="Track ID",
        ),
    },
)
def dag_update_index_for_track():

    track_id = "{{ dag_run.conf['track_id'] }}"

    track_info = get_track_info(track_id=track_id)


    # index_raw_data = generate_data_to_index(
    #     track_metadata=metadata,
    #     translated_titles=track_titles.map(lambda x: x["title"]),
    # )

    # index_words = generate_search_index(
    #     index_raw_data, languages=languages_original
    # )  # TODO: index_raw_data may contain multiple languages

    # save_search_index.partial(track_id=track_id).expand(word=index_words)




dag_update_index_for_track()
