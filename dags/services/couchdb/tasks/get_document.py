from requests import get

from airflow.decorators import task


@task(
    task_display_name="🗄️ CouchDB: Get Document")
def get_document(
    connection_string: str,
    collection: str,
    document_id: str = None
) -> None:
    url = f"{connection_string}/{collection}/{document_id}"
    response = get(url)
    if response.status_code != 200:
        return None
    return response.json()
