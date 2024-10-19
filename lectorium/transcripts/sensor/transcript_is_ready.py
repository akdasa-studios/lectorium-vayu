from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from lectorium.services import CouchDbService


class TranscriptIsReady(BaseSensorOperator):
    @apply_defaults
    def __init__(self, track_id: str, language: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__track_id = track_id
        self.__language = language

    def poke(self, context):
        transcripts_collection_name = Variable.get("transcripts_collection_name")
        database_connection_string = Variable.get("database_connection_string")
        couch_db = CouchDbService(base_url=database_connection_string)

        print("Transcripts collection name:", transcripts_collection_name)
        print("Database connection string:", database_connection_string)
        print("Document ID:", f"{self.__track_id}::{self.__language}")
        document = couch_db.find_by_id(transcripts_collection_name, f"{self.__track_id}::{self.__language}")

        print("Document:", document)

        return document is not None
