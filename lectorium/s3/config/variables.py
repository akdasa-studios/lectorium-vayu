from typing import TypedDict

from airflow.models import Variable


class AppBucketGenerateTempraryAccessKey(TypedDict):
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region_name: str
    s3_endpoint_url: str

class AppBucketAccessKey(TypedDict):
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region_name: str
    s3_endpoint_url: str


Variable.setdefault(
    "app-bucket::generate-temporary-access-key",
    AppBucketGenerateTempraryAccessKey(
        s3_access_key_id="",
        s3_secret_access_key="",
        s3_region_name="",
        s3_endpoint_url="",
    ),
    "Credentials to genereate temporary access key for the app bucket",
    deserialize_json=True)

Variable.setdefault(
    "app-bucket::access-key",
    AppBucketAccessKey(
        s3_access_key_id="",
        s3_secret_access_key="",
        s3_region_name="",
        s3_endpoint_url="",
    ),
    "Credentials to access the app bucket",
    deserialize_json=True)
