from os import environ, path

from airflow.decorators import task

from lectorium.models import Environment
from lectorium.services.storage import StorageService


@task(task_display_name="Upload to Storage")
def upload_file_to_storage(
    file_path: str,
    track_id: str,
    env: Environment,
) -> str:
    storage = StorageService(
        region=environ.get("STORAGE_REGION"),
        endpoint_url=environ.get("STORAGE_ENDPOINT_URL"),
        access_key_id=environ.get("STORAGE_ACCESS_KEY_ID"),
        access_secret_key=environ.get("STORAGE_ACCESS_SECRET_KEY"),
        public_url=environ.get("STORAGE_PUBLIC_URL"),
        bucket_name=env["audio_files_bucket_name"],
    )

    print(f"Uploading file '{file_path}' to storage")

    extension = path.splitext(file_path)[1]
    remote_file_name = f"{track_id}{extension}"
    remote_url = storage.upload_file(file_path, remote_file_name)

    print(f"File uploaded to {remote_url}")
    return remote_url
