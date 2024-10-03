from requests import get, post, put


class CouchDbService:
    def __init__(self, base_url: str) -> None:
        self.__base_url = base_url

    def save(self, database_name: str, document: dict) -> None:
        document_id = document["_id"]
        url = f"{self.__base_url}/{database_name}/{document_id}"

        response = get(url)
        revision = None
        if response.status_code == 200:
            stored_data = response.json()
            revision = stored_data.get("_rev")
            saving_data = {**document, "_rev": revision}
            if stored_data == saving_data:
                return

        if revision is None:
            response = put(url, json=document)
        else:
            response = put(url, json={**document, "_rev": revision})

        if response.status_code not in [200, 201]:
            raise Exception(f"Failed to save document: {response.text}")

    def find_by_id(self, database_name: str, document_id: str) -> dict | None:
        url = f"{self.__base_url}/{database_name}/{document_id}"
        response = get(url)
        if response.status_code != 200:
            return None
        return response.json()

    def find_by_filter(self, database_name: str, filter: dict) -> list[dict]:
        url = f"{self.__base_url}/{database_name}/_find"
        response = post(url, json={"selector": filter})
        return response.json().get("docs", [])
