from os import path

from airflow.decorators import task
from airflow.models import Variable

from lectorium.services.storage import StorageService


@task(task_display_name="⬆️ Upload to Storage")
def upload_file_to_storage(
    file_path: str,
    track_id: str,
) -> str:
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    storage = StorageService(
        region=Variable.get("storage_region"),
        endpoint_url=Variable.get("storage_endpoint_url"),
        access_key_id=Variable.get("storage_access_key_id"),
        access_secret_key=Variable.get("storage_access_secret_key"),
        public_url=Variable.get("storage_public_url"),
        bucket_name=Variable.get("audio_files_bucket_name"),
    )

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    print(f"Uploading file '{file_path}' to storage")

    extension = path.splitext(file_path)[1]
    remote_file_name = f"{track_id}{extension}"
    remote_url = storage.upload_file(file_path, remote_file_name)

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    print(f"File uploaded to {remote_url}")
    return remote_url
