import json
from typing import TypedDict

import requests


class OllamaServiceResponse(TypedDict):
    response: str


class OllamaService:
    def __init__(self, host: str, port: int):
        self.__host = host
        self.__port = port

    def generate(
        self,
        model: str,
        prompt: str,
    ):
        url = f"http://{self.__host}:{self.__port}/api/generate"

        payload = {"model": model, "prompt": prompt, "stream": False}
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, data=json.dumps(payload), headers=headers)

        return OllamaServiceResponse(**response.json())
