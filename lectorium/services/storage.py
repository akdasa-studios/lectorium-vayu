from boto3 import client


class StorageService:
    def __init__(
        self,
        access_key_id: str,
        access_secret_key: str,
        endpoint_url: str,
        region: str,
        public_url: str,
        bucket_name: str,
    ) -> None:
        self.__access_key_id = access_key_id
        self.__access_secret_key = access_secret_key
        self.__endpoint_url = endpoint_url
        self.__region = region
        self.__public_url = public_url
        self.__bucket_name = bucket_name

    def upload_file(
        self,
        local_path: str,
        reference_id: str,
    ):
        s3_client = client(
            "s3",
            region_name=self.__region,
            endpoint_url=self.__endpoint_url,
            aws_access_key_id=self.__access_key_id,
            aws_secret_access_key=self.__access_secret_key,
        )

        s3_client.upload_file(local_path, self.__bucket_name, reference_id)

        return f"{self.__public_url}:{self.__bucket_name}/{reference_id}"
