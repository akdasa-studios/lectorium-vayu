import boto3
from airflow.decorators import task
from airflow.models import Variable

from lectorium.s3 import AppBucketGenerateTempraryAccessKey, StsSessionToken


@task(task_display_name="STS: Get Session Token")
def sts_get_session_token(
    duration_seconds: int = 3600,
):
    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    creds: AppBucketGenerateTempraryAccessKey = Variable.get(
        "app-bucket::generate-temporary-access-key",
        deserialize_json=True)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # Creating an STS client with provided credentials to get temporary access key.
    sts_client = boto3.client(
        'sts',
        endpoint_url=creds['s3_endpoint_url'],
        aws_access_key_id=creds['s3_access_key_id'],
        aws_secret_access_key=creds['s3_secret_access_key'],
        region_name=creds['s3_region_name']
    )

    # Making a call to STS to get a session token with the specified duration. The
    # session token will be used to access the app bucket.
    response = sts_client.get_session_token(DurationSeconds=duration_seconds)
    credentials = response['Credentials']

    # ---------------------------------------------------------------------------- #
    #                                    Output                                    #
    # ---------------------------------------------------------------------------- #

    return StsSessionToken(
        access_key_id=credentials['AccessKeyId'],
        secret_access_key=credentials['SecretAccessKey'],
        token=credentials['SessionToken'],
        expiration=credentials['Expiration']
    )
