from openai import OpenAI


class OpenAIService:
  def __init__(self, api_key: str):
    self.__client = OpenAI(api_key=api_key)

  def process(
    self,
    request_system: str = None,
    request_user: str = None,
  ):
    response = self.__client.chat.completions.create(
      model="gpt-4o",
      messages=[
        {
          "role": "system",
          "content": [
            {
              "type": "text",
              "text": request_system
            }
          ]
        },
        {
          "role": "user",
          "content": [
            {
              "type": "text",
              "text": request_user,
            }
          ]
        },
      ],
      temperature=1,
      max_tokens=256,
      top_p=1,
      frequency_penalty=0,
      presence_penalty=0,
      response_format={
        "type": "text"
      }
    )
    return response.choices[0].message.content