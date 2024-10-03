from deepgram import DeepgramClient, PrerecordedOptions


class DeepgramService:
  def __init__(self, api_key: str):
    self.__client = DeepgramClient(api_key)

  def process(
    self,
    url: str,
    language: str,
  ):
    client = self.__client.listen.prerecorded.v('1')
    return client.transcribe_url(
      { 'url': url },
      PrerecordedOptions(
        punctuate=True,
        model="nova-2",
        language=language,
        smart_format=True,
        paragraphs=True,
      )
    )
