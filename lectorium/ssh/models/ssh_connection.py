from typing import TypedDict


class SshConnection(TypedDict):
    host: str
    port: int
    username: str
    private_key: str | None = None
