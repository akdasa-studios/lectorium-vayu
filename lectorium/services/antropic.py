from anthropic import Anthropic


class AntropicService:
  def __init__(self, api_key: str):
    self.__client = Anthropic(api_key=api_key)

  def process(
    self,
    request_system: str = None,
    request_user: str = None,
  ):
    message = self.__client.messages.with_raw_response.create(
      model="claude-3-5-sonnet-20240620",
      max_tokens=8192,
      temperature=0,
      system=request_system,
      messages=[
        {
          "role": "user",
          "content": [
            {
              "type": "text",
              "text" : request_user,
            }
          ]
        }
      ],
      extra_headers={
        "anthropic-beta": "max-tokens-3-5-sonnet-2024-07-15"
      }
    )
    return message.parse().content[0].text
