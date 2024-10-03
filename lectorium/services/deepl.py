import deepl


class DeepLService:
    def __init__(self, api_key: str):
        self.__client = deepl.Translator(api_key)

    def translate(self, text: str, language: str) -> str:
        result = self.__client.translate_text(text, target_lang=language)
        return result.text
