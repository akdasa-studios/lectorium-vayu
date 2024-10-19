from dataclasses import dataclass
from datetime import datetime


@dataclass
class StsSessionToken:
    access_key_id: str
    secret_access_key: str
    token: str
    expiration: datetime