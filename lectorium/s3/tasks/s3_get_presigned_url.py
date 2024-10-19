import boto3
from airflow.decorators import task
from airflow.models import Variable
from botocore.config import Config

from lectorium.s3 import AppBucketAccessKey


@task(task_display_name="S3: Get Presigned URL")
def s3_get_presigned_url(
    bucket_name: str,
    object_name: str,
    duration_seconds: int = 3600,
):
    object_name = object_name.replace("s3://", "")

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    creds: AppBucketAccessKey = Variable.get(
        "app-bucket::access-key",
        deserialize_json=True)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    http_method = 'GET'

    s3_client = boto3.client(
        's3',
        endpoint_url=creds['s3_endpoint_url'].replace("https://lectorium.", "https://"),
        aws_access_key_id=creds['s3_access_key_id'],
        aws_secret_access_key=creds['s3_secret_access_key'],
        region_name=creds['s3_region_name'],
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'}
        )
    )

    # Generate the presigned URL
    url = s3_client.generate_presigned_url(
        ClientMethod=f'{http_method.lower()}_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_name
        },
        ExpiresIn=duration_seconds
    )

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return url
