from lectorium.services.ollama import OllamaService


def run_prompt_with_ollama(request: str) -> str:
    # response = ollama.generate(prompt=prompt_final, model="gemma2:27b")
    ollama = OllamaService(host="47.186.36.105", port=57429)
    response = ollama.generate(prompt=request, model="llama3:8b")
    return response["response"]
