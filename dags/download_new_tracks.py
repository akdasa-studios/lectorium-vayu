from __future__ import annotations
from datetime import timedelta
from glob import glob
from os import path, rename, makedirs
import json

import pendulum

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


FOLDER = "/akd-studios/lectorium/modules/services/vayu/input/"
INBOX_PATH = "/akd-studios/lectorium/modules/services/vayu/input/inbox/**/*.mp3"
PROCRSSING_PATH = "/akd-studios/lectorium/modules/services/vayu/input/processing/"
INPUT_FOLDER_DATASET = Dataset("input")


@dag(
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lectorium"],
    dag_display_name="Download New Tracks",
)
def download_new_tracks():

    @task
    def download():
        files_to_process = glob(INBOX_PATH, recursive=True)
        new_files = []
        for file in files_to_process:
            relative_path = path.relpath(file, start=path.dirname(FOLDER))
            new_path = path.join(PROCRSSING_PATH, relative_path)
            new_dir = path.dirname(new_path)
            new_files.append(new_path)
            if not path.exists(new_dir):
                makedirs(new_dir)
            rename(file, new_path)

        return new_files

        # # TODO: get first N files
        # processed_files = []
        # for file in files_to_process:
        #     new_path = path.join(PROCRSSING_PATH, path.basename(file))
        #     rename(file, new_path)
        #     processed_files.append(new_path)
        # return processed_files

    @task
    def start_processing(file: str, **kwargs):
        # load configuration from the config.json file
        # which is located in the same folder as the file
        # that is being processed
        folder = path.dirname(file)
        config_file = path.join(folder, "config.json")
        config_data = {}

        if path.exists(config_file):
            with open(config_file, "r") as f:
                config_data = json.load(f)
        else:
            config_data = {}

        # trigger the process_track DAG to process the file
        # with the configuration data
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_target_dag",
            trigger_dag_id="process_track",
            conf={"file_path": file, **config_data},
            reset_dag_run=False,
            wait_for_completion=False,
        )
        trigger.execute(context=kwargs)

    # Define the DAG structure
    files = download()
    start_processing.expand(file=files)


download_new_tracks()
